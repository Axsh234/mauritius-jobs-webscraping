import requests
from bs4 import BeautifulSoup
import mysql.connector
import time
import logging
import csv

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("scrapnalyze.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# DB connection
db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="jobs_db"
)
cursor = db.cursor(dictionary=True)

# Create table if needed
cursor.execute("""
CREATE TABLE IF NOT EXISTS scrapnalyze_job_mate (
    id INT AUTO_INCREMENT PRIMARY KEY,
    job_id INT UNIQUE,
    title VARCHAR(255),
    sector VARCHAR(255),
    employer VARCHAR(255),
    country VARCHAR(255),
    closing_date VARCHAR(255),
    summary TEXT
)
""")

base_url = "https://mauritiusjobs.govmu.org/"
session = requests.Session()
headers = {"User-Agent": "Mozilla/5.0"}

# Sets to track status
open_job_ids = set()
new_jobs = []
existing_jobs = []
removed_jobs = []

def extract_job_summary(soup, job_id):
    hidden_rows = soup.find_all("tr", class_="hidden")
    for hr in hidden_rows:
        div = hr.find("div", id=job_id)
        if div:
            details_table = div.find("table", class_="job_details")
            if details_table:
                for tr in details_table.find_all("tr"):
                    tds = tr.find_all("td")
                    if len(tds) >= 2 and tds[0].get_text(strip=True) == "Job Summary":
                        return tds[1].get_text(strip=True)
    return ""

# Total pages
resp = session.get(base_url, headers=headers)
soup = BeautifulSoup(resp.text, "html.parser")
pages_input = soup.find("input", {"id": "pages"})
total_pages = int(pages_input["value"]) if pages_input else 1

logging.info(f"Total pages found: {total_pages}")

# Process all pages
for page in range(1, total_pages + 1):
    page_url = base_url if page == 1 else f"{base_url}?page={page}"
    logging.info(f"Processing page {page}: {page_url}")

    resp = session.get(page_url, headers=headers)
    if resp.status_code != 200:
        logging.warning(f"Failed to load page {page}")
        continue

    soup = BeautifulSoup(resp.text, "html.parser")
    job_rows = soup.find_all("tr", onclick=True)

    logging.info(f"Found {len(job_rows)} jobs on page {page}")

    for row in job_rows:
        cols = row.find_all("td")
        if len(cols) >= 6:
            try:
                job_id = int(row["onclick"].split("'")[1])
                open_job_ids.add(job_id)

                title = cols[1].get_text(strip=True)
                sector = cols[2].get_text(strip=True)
                employer = cols[3].get_text(strip=True)
                country = cols[4].get_text(strip=True)
                closing_date = cols[5].get_text(strip=True)
                summary = extract_job_summary(soup, str(job_id))

                cursor.execute("""
                    INSERT IGNORE INTO scrapnalyze_job_mate
                    (job_id, title, sector, employer, country, closing_date, summary)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (job_id, title, sector, employer, country, closing_date, summary))
                db.commit()

                if cursor.rowcount > 0:
                    new_jobs.append({
                        "id": None,  # Will fill later if needed
                        "job_id": job_id, "title": title, "sector": sector,
                        "employer": employer, "country": country,
                        "closing_date": closing_date, "summary": summary
                    })
                    logging.info(f"➕ Added new: {job_id} | {title}")
                else:
                    cursor.execute("SELECT * FROM scrapnalyze_job_mate WHERE job_id = %s", (job_id,))
                    row_data = cursor.fetchone()
                    if row_data:
                        existing_jobs.append(row_data)

            except Exception as e:
                logging.error(f"Error: {e}")

    time.sleep(1)  # polite delay

# Remove closed jobs
cursor.execute("SELECT * FROM scrapnalyze_job_mate")
db_jobs = cursor.fetchall()

for row in db_jobs:
    if row["job_id"] not in open_job_ids:
        removed_jobs.append(row)
        cursor.execute("DELETE FROM scrapnalyze_job_mate WHERE job_id = %s", (row["job_id"],))
        db.commit()
        logging.info(f"❌ Removed closed: {row['job_id']}")

# Write CSVs
def write_csv(filename, data, fieldnames):
    with open(filename, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in data:
            writer.writerow(row)

fieldnames = ["id", "job_id", "title", "sector", "employer", "country", "closing_date", "summary"]

if new_jobs:
    # Fill 'id' by requery if needed
    for job in new_jobs:
        cursor.execute("SELECT id FROM scrapnalyze_job_mate WHERE job_id = %s", (job["job_id"],))
        res = cursor.fetchone()
        if res:
            job["id"] = res["id"]
    write_csv("new_jobs.csv", new_jobs, fieldnames)

if existing_jobs:
    write_csv("existing_jobs.csv", existing_jobs, fieldnames)

if removed_jobs:
    write_csv("removed_jobs.csv", removed_jobs, fieldnames)

logging.info(f"✅ New jobs: {len(new_jobs)}, Existing: {len(existing_jobs)}, Removed: {len(removed_jobs)}")

cursor.close()
db.close()
logging.info("🎉 Done — logs and CSVs saved.")
