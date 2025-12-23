import threading
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from playwright.sync_api import sync_playwright, TimeoutError
from fastapi import FastAPI, Query
import uvicorn

from pymongo import MongoClient, ASCENDING

# ================= CONFIG =================
URL = "https://enam.gov.in/web/trading-details"
MONGO_URI = "mongodb+srv://farmers_db:Farmers_db@cluster0.x0rzrqo.mongodb.net/"
DB_NAME = "mandi_db"
COLLECTION = "mandi_prices"

MAX_WORKERS = 6
SCRAPE_INTERVAL_SECONDS = 2 * 60 * 60
# =========================================

app = FastAPI(
    title="Mandi Price API",
    description="Scraped eNAM mandi prices served via API",
    version="2.0.0"
)

scrape_status = {
    "running": False,
    "total_states": 0,
    "completed_states": 0,
    "total_rows": 0,
    "percentage": 0,
    "last_run": None
}
status_lock = threading.Lock()

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION]

collection.create_index(
    [("state", ASCENDING), ("apmc", ASCENDING), ("commodity", ASCENDING), ("date", ASCENDING)],
    unique=True
)

def get_all_states():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(URL, timeout=60000)
        page.wait_for_timeout(3000)
        states = page.eval_on_selector_all(
            "#min_max_state option",
            """
            opts => opts.map(o => o.label.trim())
                        .filter(l => l && l !== 'Select State' && l !== '--All--')
            """
        )
        browser.close()
        return states

def scrape_state(state):
    results = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(URL, timeout=60000)
        page.wait_for_timeout(2000)
        try:
            page.select_option("#min_max_state", label=state)
            page.wait_for_timeout(3000)
            page.wait_for_function(
                "() => document.querySelector('#min_max_apmc').options.length > 1"
            )
            apmcs = page.eval_on_selector_all(
                "#min_max_apmc option",
                """
                opts => opts.map(o => o.label.trim())
                            .filter(l => l && l !== 'Select APMC')
                """
            )
            for apmc in apmcs:
                try:
                    page.select_option("#min_max_apmc", label=apmc)
                    page.select_option("#min_max_commodity", value="0")
                    page.click("#today_mandi_refresh")
                    page.wait_for_selector("#mandi_table tr", timeout=20000)
                    rows = page.locator("#mandi_table tr")
                    for i in range(rows.count()):
                        cols = rows.nth(i).locator("td").all_inner_texts()
                        if len(cols) == 10:
                            results.append({
                                "state": state.upper(),
                                "apmc": cols[0].upper(),
                                "commodity": cols[1].upper(),
                                "min_price": cols[2],
                                "modal_price": cols[3],
                                "max_price": cols[4],
                                "arrivals": cols[5],
                                "traded": cols[6],
                                "unit": cols[7],
                                "date": cols[8],
                                "source": "eNAM",
                                "scraped_at": datetime.utcnow()
                            })
                except TimeoutError:
                    continue
        except TimeoutError:
            pass
        browser.close()
    return results

def run_scraper():
    print("🚀 Scraping started")
    states = get_all_states()
    with status_lock:
        scrape_status.update({
            "running": True,
            "total_states": len(states),
            "completed_states": 0,
            "total_rows": 0,
            "percentage": 0,
            "last_run": datetime.utcnow().isoformat()
        })
    all_docs = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(scrape_state, s): s for s in states}
        for future in as_completed(futures):
            docs = future.result()
            all_docs.extend(docs)
            with status_lock:
                scrape_status["completed_states"] += 1
                scrape_status["total_rows"] += len(docs)
                scrape_status["percentage"] = round(
                    (scrape_status["completed_states"] / scrape_status["total_states"]) * 100, 2
                )
    for doc in all_docs:
        collection.update_one(
            {
                "state": doc["state"],
                "apmc": doc["apmc"],
                "commodity": doc["commodity"],
                "date": doc["date"]
            },
            {"$set": doc},
            upsert=True
        )
    with status_lock:
        scrape_status["running"] = False
    print(f"✅ Scraping completed | Rows: {len(all_docs)}")

def scheduler():
    while True:
        try:
            run_scraper()
        except Exception as e:
            print("❌ Scraper error:", e)
        time.sleep(SCRAPE_INTERVAL_SECONDS)

@app.get("/")
def root():
    return {"message": "Mandi Price API running"}

@app.get("/scrape-status")
def scrape_status_api():
    with status_lock:
        return scrape_status

@app.get("/mandi")
def mandi(state: str | None = Query(None),
          apmc: str | None = Query(None),
          commodity: str | None = Query(None)):
    q = {}
    if state: q["state"] = state.upper()
    if apmc: q["apmc"] = apmc.upper()
    if commodity: q["commodity"] = {"$regex": commodity.upper()}
    data = list(collection.find(q, {"_id": 0}))
    return data if data else {"status": "no_data"}

@app.get("/states")
def states():
    return collection.distinct("state")

@app.get("/apmcs")
def apmcs(state: str):
    return collection.distinct("apmc", {"state": state.upper()})

if __name__ == "__main__":
    t = threading.Thread(target=scheduler, daemon=True)
    t.start()
    uvicorn.run(app, host="0.0.0.0", port=8000)
