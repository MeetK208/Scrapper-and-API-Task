from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
import requests
from bs4 import BeautifulSoup
from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# Database setup
engine = create_engine('sqlite:///books.db')
Base = declarative_base()

# SQLAlchemy ORM model
class Book(Base):
    __tablename__ = 'book'
    id = Column(Integer, primary_key=True)
    title = Column(String(200), nullable=False)
    author = Column(String(200), nullable=False)
    Publishedyear = Column(Integer, nullable=False)
    createdDatetime = Column(DateTime, default=datetime.utcnow)

# Create tables
Base.metadata.create_all(engine)

# Create session
Session = sessionmaker(bind=engine)
session = Session()

def scrape_books():
    logging.info("Scraping started...")
    for i in range(1, 10):
        url = f"https://openlibrary.org/trending/daily?page={i}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            for item in soup.findAll('div', attrs={'class': 'details'}):
                bookName = item.find('div', attrs={'class': 'resultTitle'}).get_text(strip=True)
                authorName = item.find('span', attrs={'class': 'bookauthor'}).get_text(strip=True)
                Published_year = item.find('span', attrs={'class': 'publishedYear'})
                
                if authorName.startswith('by'):
                    authorName = authorName[2:].strip()
                
                if Published_year:
                    Published_year = int(Published_year.get_text(strip=True).split()[-1])
                else:
                    Published_year = 0
                
                new_book = Book(title=bookName, author=authorName, Publishedyear=Published_year)
                session.add(new_book)
            
            session.commit()
            logging.info(f"Scraping successful at {datetime.now()}.")
        except (requests.RequestException, SQLAlchemyError) as e:
            session.rollback()
            logging.error(f"Error during scraping: {e}")

def schedule_scraping():
    logging.info("Scheduler started...")
    scheduler = BlockingScheduler()
    scheduler.add_job(scrape_books, 'interval', minutes=2)
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logging.info("Scheduler stopped.")

if __name__ == '__main__':
    schedule_scraping()
