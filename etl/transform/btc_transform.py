import os
from datetime import datetime

from etl.transform.database_transform import DBConnectorTransform
from etl.transform.utils_transform import (
    move_file,
    load_json_file,
)
from etl.transform.logger_transform import logger


class BitcoinTransform:
    """
    Handles transformation of raw Bitcoin API data files into structured records
    and loads them into the database.

    Methods:
        __init__()   -- Initializes with a DB connection
        transform()  -- Processes and loads data from a single file
        call()       -- Triggers processing of all files based on import_log

    Instance Variables:
        conn      -- Database connection object (DBConnectorTransform)

    """

    def __init__(self, conn: DBConnectorTransform):
        """
        Initialize the transformer with a database connection.

        Parameters:
            conn -- DBConnectorTransform instance for DB operations
        """
        self.conn = conn

    def transform(self, file_info):
        """
        Process a single Bitcoin data file, transform its contents, and insert into DB.

        Parameters:
            file_info -- Dictionary containing file information from import_log

        Returns:
            None
        """
        file_path = file_info['full_path']
        import_log_id = file_info['id']

        logger.info(f"Processing bitcoin file: {file_path} (import_log_id: {import_log_id})")
        data_type = "bitcoin"
        status = "error"
        processed_count = 0
        currency_id = file_info['currency_id']

        try:
            data_list = load_json_file(file_path)

            if data_list is None:
                return

            for data in data_list:
                meta = data.get("Meta Data", {})
                if not meta:
                    logger.warning(f"No Meta Data found in file: {file_path}")
                    continue

                currency_code = meta.get("4. Market Code", "")
                if not currency_code:
                    logger.warning(
                        f"No Market Code found in file: {file_path}"
                    )
                    continue

                logger.debug(
                    f"Processing data for currency code: {currency_code}"
                )

                if not currency_id:
                    currency_id = self.conn.get_currency_by_code(currency_code)

                if not currency_id:
                    logger.warning(
                        f"No currency ID found for code: {currency_code}"
                    )
                    continue

                time_series = data.get(
                    "Time Series (Digital Currency Daily)", {}
                )
                if not time_series:
                    logger.warning(
                        f"No Time Series data found in file: {file_path}"
                    )
                    continue

                logger.debug(
                    f"Processing {len(time_series)} time series entries"
                )
                for date_str, daily_data in time_series.items():
                    try:
                        date = datetime.strptime(date_str, "%Y-%m-%d").date()
                        logger.debug(f"Processing data for date: {date}")

                        open_price = float(daily_data.get("1. open"))
                        high = float(daily_data.get("2. high"))
                        low = float(daily_data.get("3. low"))
                        close = float(daily_data.get("4. close"))
                        volume = float(daily_data.get("5. volume"))

                        logger.debug(
                            f"Upserting bitcoin data for currency_id {currency_id}, date {date}"
                        )
                        self.conn.upsert_btc_data(
                            currency_id,
                            date,
                            open_price,
                            high,
                            low,
                            close,
                            volume,
                        )

                        processed_count += 1
                    except Exception as e:
                        logger.error(
                            f"Error processing date {date_str}: {str(e)}",
                            exc_info=True,
                        )

            if processed_count > 0:
                status = "processed"
                logger.info(
                    f"Successfully processed {processed_count} data points from file: {file_path}"
                )
            else:
                logger.warning(f"No data processed from file: {file_path}")

        except Exception as e:
            logger.error(
                f"Error processing file {file_path}: {str(e)}", exc_info=True
            )
            status = "error"

        new_file_path = move_file(status, data_type, file_path)

        logger.info(f"Logging transformation for file {file_path}")
        self.conn.log_transform(
            currency_id,
            os.path.dirname(new_file_path),
            os.path.basename(new_file_path),
            processed_count,
            status
        )

    def call(self):
        """
        Trigger the transformation process for bitcoin files based on import_log.

        Returns:
            None
        """
        logger.info("Starting Bitcoin transformation process")

        files_to_process = self.conn.get_files_to_process("bitcoin")

        if not files_to_process:
            logger.info("No bitcoin files found in import_log to process")
            return

        logger.info(f"Processing {len(files_to_process)} bitcoin files")

        for file_info in files_to_process:
            self.transform(file_info)

        logger.info("Bitcoin transformation process complete")
