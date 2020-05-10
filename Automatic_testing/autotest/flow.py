import luigi
import datetime

from autotest import actives


class MainFlow(actives.Activity):
    """
    main setp entry point. run task day by date
    """

    @classmethod
    def familyName(cls):
        return "autotest.mainentry"
    
    def requires(self):
        return actives.Activity3rdRollbackDatabase()
            
    def runIt(self):
        startDate = self.ctx.task.startDate
        endDate = self.ctx.task.endDate 
 
        date_array = (startDate + datetime.timedelta(days=x) for x in range(0, (endDate - startDate).days + 1))
        for date in date_array:
            self.logger.info("run daily task on %s", date)
            luigi.build([DailyFlow(date=date)], workers=1, local_scheduler=True)
        self.logger.info("all tasks have been done")

    

class DailyFlow(actives.DailyActivity):
    """
    daily step entry point. run pseudo bsp task in a day
    """       

    @classmethod
    def familyName(cls):
        return "autotest.dailyentry"
    
    def requires(self):
        return actives.DailyActivity6thRunJobs(date=self.date)
            
    def runIt(self):
        self.logger.info("run task in %s", self.date)
        
        luigi.build([ 
            actives.DailyActivity7thCompareBalance(date=self.date),
            actives.DailyActivity8thCompareBilling(date=self.date),
            actives.DailyActivity9thCompareCsi(date=self.date),
            actives.DailyActivity10thCompareHot(date=self.date)
        ], workers=1, local_scheduler=True)
        
        self.logger.info("tasks on %s have been done", self.date)
