import luigi
import logging.config

from autotest import flow

if __name__ == '__main__':
    logging.config.fileConfig("logging.conf")
    luigiRunResult = luigi.build([flow.MainFlow()], workers=1, local_scheduler=True, detailed_summary=True, logging_conf_file="logging.conf")
    if luigiRunResult.scheduling_succeeded:
        logging.info("mission accomplished.")
    else:
        logging.error("mission failed -\n\t%s", luigiRunResult.summary_text)

