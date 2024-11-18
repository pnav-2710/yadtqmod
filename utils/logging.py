import logging

def setup_logging(module_name:str):
    # Create a logger
    logger = logging.getLogger(module_name)
    if not logger.hasHandlers():
        logger.setLevel(logging.DEBUG)  # Set the minimum log level (DEBUG, INFO, WARNING, etc.)

        # Create a file handler that overwrites the log file each time
        file_handler = logging.FileHandler( "app.log", mode="w")
        file_handler.setLevel(logging.DEBUG)  # Set the minimum log level for this handler

        # Create a formatter and set it for the handler
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(formatter)

        # Add the handler to the logger
        logger.addHandler(file_handler)

        # Optional: add a console handler for output to the terminal as well
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger

