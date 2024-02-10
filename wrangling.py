from models.models import Pipeline
import datetime
import logging

logging.basicConfig(level=logging.DEBUG,
                    filename=f"logs/{datetime.datetime.now().strftime('%Y%m%d-%H%M%S')}.log",
                    filemode="w",
                    format="%(asctime)s: %(levelname)s - %(message)s")

if __name__ == "__main__":
    pipline = Pipeline()
    pipline.wrangling()
