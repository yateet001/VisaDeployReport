import asyncio
import logging
from EmbedFastReportAutomation.runner import ReportsRunner

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    runner = ReportsRunner()
    asyncio.run(runner.run())

if __name__ == "__main__":
    main()