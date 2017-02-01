import glob
import os

from bs4 import BeautifulSoup
from cStringIO import StringIO
from pdfminer.converter import HTMLConverter
from pdfminer.layout import LAParams
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.pdfpage import PDFPage
from sqlalchemy import create_engine, Column, Integer, String, Date
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

global connect_string
# connect_string='postgresql://vlad:Uwg1x6NQ@unstruct.c67lxkbcllck.ap-southeast-2.rds.amazonaws.com:5862/datastore'
connect_string = 'postgresql://unstruct_admin:jazzylittlelemming@unstruct.c67lxkbcllck.ap-southeast-2.rds.amazonaws.com:5862/datastore'


# connect_string='postgresql://postgres:anticip8tion@localhost:5432/vlad'

def convert_pdf_to_html(path):
    # Convert pdf to HTML format using PDFMiner
    rsrcmgr = PDFResourceManager()
    retstr = StringIO()
    codec = 'utf-8'
    laparams = LAParams()
    device = HTMLConverter(rsrcmgr, retstr, codec=codec, laparams=laparams)
    fp = file(path, 'rb')
    interpreter = PDFPageInterpreter(rsrcmgr, device)
    password = ""
    maxpages = 0  # is for all
    caching = True
    pagenos = set()
    for page in PDFPage.get_pages(fp, pagenos, maxpages=maxpages, password=password, caching=caching,
                                  check_extractable=True):
        interpreter.process_page(page)
    fp.close()
    device.close()
    str = retstr.getvalue()
    retstr.close()
    return str


def Lists_of_elements():
    key_uet = {
        1: "Is the industry structure facing the firm likely to improve or deteriorate over the next year? (getting worse, no change, getting better):",
        2: "Is the regulatory/government environment facing the firm likely to improve or deteriorate over the next year? (getting tougher, no change, getting better, no view):",
        3: "Are capex levels likely to significantly fall for this firm over the next year? (capex likely to increase, no change, capex likely to fall):",
        4: "How exposed are the earnings of the stock to a strong AUDUSD? (high AUDUSD bad for earnings, neutral, high AUDUSD good for earnings):",
        5: "What is the probability of the firm issuing >5% equity over the next six months? (unlikely, highly likely, no view):",
        6: "Over the last 3-6 months in broad terms have things been improving/no change/getting worse for this stock? (getting a lot worse, not much change, getting a lot better, no view):",
        7: "Relative to the current CONSENSUS earnings forecast, is the next company earnings update likely to lead to (negative surprise vs consensus, in-line with consensus, positive surprise vs consensus expectations, no view):",
        8: "Relative to YOUR current earnings forecast, is there relatively greater risk at the next earnings result of: (downside skew risk to earnings, equal upside or downside risk to earnings, upside skew risk to earnings, no view):",
        9: "What is the probability of the firm announcing a new stock buyback program over the next six months? (unlikely, highly likely, no view):"
    }

    key_AnalystSurvey = {1: "better):",
                         2: "no change, getting better, no view):",
                         3: "fall):",
                         4: "good for earnings):",
                         5: "What is the probability of the firm issuing >5% equity over the next six months? (unlikely, highly likely, no view):",
                         6: "worse, not much change, getting a lot better, no view):",
                         7: "surprise vs consensus, in-line with consensus, positive surprise vs consensus expectations, no view):",
                         8: "to earnings, equal upside or downside risk to earnings, upside skew risk to earnings, no view):",
                         9: "no view):"
                         }
    # Get current folder
    cd = dir_path = os.path.dirname(os.path.realpath(__file__))
    list_of_pdf = []

    # Get files in current folder
    Files = glob.glob(cd + "/*.pdf")

    for file_n in Files:
        list_of_pdf.append(file_n)

    return key_uet, key_AnalystSurvey, list_of_pdf


def write_to_db(sessionsql, file_name2, file_date, name, ticket, par_dict, key_uet):
    for key in key_uet.keys():
        newrow = PDF_M(file_name=file_name2,
                       file_date=file_date,
                       stock_code=ticket,
                       stock_name=name,
                       Bullet_name=key_uet[key],
                       Bullet_value=par_dict[key]
                       )

        sessionsql.add(newrow)
    try:
        sessionsql.commit()
    except SQLAlchemyError as e:
        sessionsql.rollback()
        print(str(e))


engine = create_engine(connect_string)
# Declare an instance of the Base class for mapping tables
Base = declarative_base()


# Create class that contain all info about columns in DB, to add new colum, includi it here
class PDF_M(Base):
    __tablename__ = 'pdf_miner'
    __table_args__ = {'schema': 'dbo'}

    id = Column(Integer, primary_key=True)
    file_name = Column('file_name', String, nullable=True)
    file_date = Column('file_date', Date, nullable=True)
    stock_code = Column('stock_code', String, nullable=True)
    stock_name = Column('stock_name', String, nullable=True)
    Bullet_name = Column('bullet_name', String, nullable=True)
    Bullet_value = Column('bullet_value', String, nullable=True)


# Create the table using the metadata attribute of the base class
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
sessionsql = Session()

lists = Lists_of_elements()
key_uet = lists[0]
key_AnalystSurvey = lists[1]
list_of_pdf = ['/Users/connahcutbush/Documents/code/github/qShares/survey/MQG-Jan-17.pdf',
               '/Users/connahcutbush/Documents/code/github/qShares/survey/UBS-Jan-17.pdf']

print
list_of_pdf
file_date = '20170101'

for pdfs in list_of_pdf:

    file_name = pdfs.split("/")[-1]
    print
    'processing file:', file_name
    par_dict = {}

    parsed_pdf = convert_pdf_to_html(pdfs)
    soup = BeautifulSoup(parsed_pdf, "html.parser")
    token = soup.findAll("div")

    for t in token:

        # print t
        row_string = t.text.replace("\n", "").strip().replace("  ", " ")
        if "Is the industry" in row_string:

            try:
                print
                "write to db", name, file_date, ticket, par_dict.values()
                write_to_db(sessionsql, file_name, file_date, name, ticket, par_dict, key_uet)
                print

            except:
                print
                "First run"
                par_dict = {}
                for el_key in key_uet.keys():
                    par_dict[el_key] = ""

            if "-" in row_string:
                # print row_string
                name_comp = row_string.split("Is the industry")[0]
                ticket = name_comp.split("-")[0].strip()
                name = name_comp.split("-")[1].split("Is the")[0]
                # print name, ticket
            else:
                name_comp = row_string.split("Is the industry")[0]
                name = name_comp.split("(")[0].strip()
                ticket = name_comp.split("(")[1].split(")")[0]
                # print name, ticket

        for g in key_uet.keys():

            if key_uet[g] in row_string:
                par_dict[g] = row_string.split(key_uet[g])[1].strip()
                break

            if key_AnalystSurvey[g] in row_string:
                par_dict[g] = row_string.split(key_AnalystSurvey[g])[1].strip()
                break

        try:
            sessionsql.commit()
        except SQLAlchemyError as e:
            sessionsql.rollback()
            print(str(e))
