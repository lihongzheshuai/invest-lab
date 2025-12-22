from datetime import datetime, date
import asyncio
import concurrent.futures

def run_async_loop(coro):
    """
    Safely run an async coroutine from a synchronous context.
    Handles cases where an event loop might already be running.
    """
    try:
        # Check if an event loop is already running in this thread
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        # If a loop is running, we cannot use asyncio.run() or loop.run_until_complete() directly.
        # We execute the coroutine in a separate thread to avoid blocking the existing loop
        # or triggering "RuntimeError: asyncio.run() cannot be called from a running event loop".
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(lambda: asyncio.run(coro))
            return future.result()
    else:
        # No loop running, safe to use asyncio.run()
        return asyncio.run(coro)

def get_latest_report_quarter(current_date: date = None) -> tuple[int, int]:
    """
    Determines the latest available fund quarterly report based on the current date.
    
    General Disclosure Deadlines (China Mutual Funds):
    - Q1 (Jan-Mar): April 22
    - Q2 (Apr-Jun): August 31
    - Q3 (Jul-Sep): October 26
    - Q4 (Oct-Dec): March 31 (Next Year)
    
    Returns:
        (year, quarter)
    """
    if current_date is None:
        current_date = date.today()
        
    year = current_date.year
    month = current_date.month
    day = current_date.day
    
    # Logic based on disclosure deadlines
    if (month < 3) or (month == 3 and day <= 31):
        # Jan 1 - Mar 31: Latest is Q3 of previous year (Q4 reports not due until Mar 31)
        # However, as soon as Mar 31 passes, Q4 is mandatory. 
        # But often Q4 reports start appearing in Jan. 
        # Strict logic: Before Mar 31, we can definitely count on Q3 Prev Year.
        # After Mar 31, Q4 Prev Year is mandatory.
        return year - 1, 3
    
    elif (month == 4 and day < 22):
        # Apr 1 - Apr 21: Latest is Q4 of previous year
        return year - 1, 4
        
    elif (month == 4 and day >= 22) or (month < 8) or (month == 8 and day < 31):
        # Apr 22 - Aug 30: Latest is Q1 of current year
        return year, 1
        
    elif (month == 8 and day >= 31) or (month < 10) or (month == 10 and day < 26):
        # Aug 31 - Oct 25: Latest is Q2 of current year
        return year, 2
        
    elif (month == 10 and day >= 26) or (month <= 12):
        # Oct 26 - Dec 31: Latest is Q3 of current year
        return year, 3
        
    return year, 3 # Fallback
