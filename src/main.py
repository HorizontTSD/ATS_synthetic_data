# from src.services.test_simulating_data_updates import run_forever
from src.services.real_simulating_data_updates import run_forever


if __name__ == "__main__":
    run_forever(interval_seconds=60)