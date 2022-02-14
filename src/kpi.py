import luigi
import pandas as pd
from datetime import timedelta
from purchase_orders import PoFinal
from quality_notifications import QnFinal


def year_week(date):
    return f'{date.year}-{date.isocalendar()[1]}'


class KpiDeliveries(luigi.Task):
    '''
    Count nº of on time deliveries for lines scheduled for last week

        - date_stat: expected date
    '''
    date = luigi.Parameter()

    def requires(self):
        return [PoFinal(date=self.date)]

    def output(self):
        file = f'data/output/{year_week(self.date)}/kpi_deliveries.csv'
        return luigi.LocalTarget(file)

    def run(self):
        file = self.input()[0].path
        usecols = ['supplier', 'area', 'buyer', 'program', 'delivered', 'date_stat']
        df = pd.read_csv(file, usecols=usecols)
        df['date_stat'] = pd.to_datetime(df['date_stat'], errors='coerce')

        # On time delivery
        start_week = pd.to_datetime(self.date - timedelta(weeks=1) - timedelta(days=self.date.weekday()))
        end_week = pd.to_datetime(self.date - timedelta(days=self.date.weekday()))

        by = ['supplier', 'area', 'buyer', 'program']
        df = df[(df['date_stat'] >= start_week) & (df['date_stat'] <= end_week)]
        df['delivered'] = df['delivered'].replace(False, None)
        df = df.groupby(by).count()
        df = df.rename(columns={'date_stat': 'scheduled'})

        with self.output().temporary_path() as temp_out_path:
            df.to_csv(temp_out_path)


class KpiLateParts(luigi.Task):
    '''
    Count nº of late lines
    '''
    date = luigi.Parameter()

    def requires(self):
        return [PoFinal(date=self.date)]

    def output(self):
        file = f'data/output/{year_week(self.date)}/kpi_late_parts.csv'
        return luigi.LocalTarget(file)

    def run(self):
        file = self.input()[0].path
        usecols = ['supplier', 'area', 'buyer', 'program', 'delivered', 'date_stat']
        df = pd.read_csv(file, usecols=usecols,
            dtype={'delivered': bool})
        df['date_stat'] = pd.to_datetime(df['date_stat'], errors='coerce')

        # Count late lines
        by = ['supplier', 'area', 'buyer', 'program']        
        df = df[(df['date_stat'] < pd.to_datetime(self.date)) & (df['delivered'] == False)]
        df = df.groupby(by).count()
        df = df.rename(columns={'delivered': 'late'})
        df = df.drop(columns=['date_stat'])

        with self.output().temporary_path() as temp_out_path:
            df.to_csv(temp_out_path)

class KpiCommentRate(luigi.Task):
    '''
    Rate of comments following guidelines
    '''
    date = luigi.Parameter()

    def requires(self):
        return [PoFinal(date=self.date)]

    def output(self):
        file = f'data/output/{year_week(self.date)}/kpi_comment_rate.csv'
        return luigi.LocalTarget(file)

    def run(self):
        file = self.input()[0].path
        usecols = ['supplier', 'area', 'buyer', 'program', 'comment_valid']
        df = pd.read_csv(file, usecols=usecols,
            dtype={'comment_valid': bool})

        # Count valid comments lines
        by = ['supplier', 'area', 'buyer', 'program']
        df['total_lines'] = 1
        df = df.groupby(by).sum()

        with self.output().temporary_path() as temp_out_path:
            df.to_csv(temp_out_path)

class KpiRejections(luigi.Task):
    '''
    Count nº of quality incidences
    '''
    date = luigi.Parameter()

    def requires(self):
        return [
            QnFinal(date=self.date)
        ]

    def output(self):
        file = f'data/output/{year_week(self.date)}/kpi_rejections.csv'
        return luigi.LocalTarget(file)

    def run(self):
        # Get quality notifications
        df = pd.read_csv(self.input()[0].path)
        by = ['supplier', 'area', 'buyer', 'program']
        df = df.groupby(by).count()['po'].reset_index()
        df = df.rename(columns={'po': 'rejections'})
        with self.output().temporary_path() as temp_out_path:
            df.to_csv(temp_out_path)

class KpiAll(luigi.Task):
    '''
    Compile all KPIs into a single file
    '''
    date = luigi.DateParameter()

    def requires(self):
        return [
            KpiDeliveries(date=self.date),
            KpiLateParts(date=self.date),
            KpiRejections(date=self.date),
            KpiCommentRate(date=self.date)
            ]

    def output(self):
        file = f'data/output/{year_week(self.date)}/kpi_all.csv'
        return luigi.LocalTarget(file)

    def run(self):
        index = ['supplier', 'area', 'buyer', 'program']
        dfs = []
        for input in self.input():
            df = pd.read_csv(input.path, index_col=index)
            dfs.append(df)
        dfs = pd.concat(dfs)
        dfs = dfs.fillna(0)
        with self.output().temporary_path() as temp_out_path:
            dfs.to_csv(temp_out_path)


class KpiAllWeeks(luigi.Task):
    '''
    Compile all KPIs into a single file
    '''
    date_start = luigi.DateParameter()
    date_end = luigi.DateParameter()

    def requires(self):
        date = self.date_start
        requirements = []
        while date <= self.date_end:
            date += pd.to_timedelta(1, unit='W')
            requirements.append(KpiAll(date=date))
        return requirements

    def output(self):
        file = f'data/output/kpi_all_weeks.csv'
        return luigi.LocalTarget(file)

    def run(self):
        dfs = []
        for i, input in enumerate(self.input()):
            df = pd.read_csv(input.path)
            date = self.requires()[i].date
            week = year_week(date)
            df['week'] = week
            dfs.append(df)
        dfs = pd.concat(dfs)
        dfs = dfs.fillna(0)
        with self.output().temporary_path() as temp_out_path:
            dfs.to_csv(temp_out_path)  

if __name__ == '__main__':

    date_start = pd.to_datetime('11/05/2020', format='%d/%m/%Y')
    date_end = pd.to_datetime('01/07/2020', format='%d/%m/%Y')
    luigi.build([KpiAllWeeks(date_start=date_start, date_end=date_end)])