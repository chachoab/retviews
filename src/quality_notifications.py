import luigi
import os
import pandas as pd
import json
import re
from datetime import timedelta, datetime
from purchase_orders import PoFinal

def year_week(date):
    return f'{date.year}-{date.isocalendar()[1]}'

class QualityNotifications(luigi.Task):
    '''
    Quality notifications (QN) are incidences detected on the Part Numbers
    upon delivery. The incidence can affect only part of the delivery.
    '''
    date = luigi.DateParameter()

    def output(self):
        file = f'data/output/{year_week(self.date)}/quality_notifications.csv'
        return luigi.LocalTarget(file)

    def run(self):
        '''
        Read QN's, rename and discard delivered
        '''
        file = f'data/input/{year_week(self.date)}/qm11.xlsx'
        rename = {
            'Documento compras': 'po',
            'Pos.docum.compras': 'pos',
            'Cantidad reclamada': 'qty'
        }
        usecols = rename.keys()
        df = pd.read_excel(file, usecols=usecols)
        df = df.rename(columns=rename)
        with self.output().temporary_path() as temp_out_path:
            df.to_csv(temp_out_path, index=False)

class QnFinal(luigi.Task):
    '''
    Retrieve some information from the PO
    '''
    date = luigi.DateParameter()

    def requires(self):
        return [
            PoFinal(date=self.date),
            QualityNotifications(date=self.date)
        ]

    def output(self):
        file = f'data/output/{year_week(self.date)}/qn_final.csv'
        return luigi.LocalTarget(file)

    def run(self):
        # Join PO's and QN's
        index = ['po', 'pos']
        usecols = ['po', 'pos', 'ev', 'supplier_id', 'supplier', 'program']        
        po = pd.read_csv(self.input()[0].path, usecols=usecols)
        po = po.drop_duplicates(subset=index)
        po = po.set_index(index)
        qn = pd.read_csv(self.input()[1].path, index_col=index)
        df = qn.join(po, how='inner')

        with self.output().temporary_path() as temp_out_path:
            df.to_csv(temp_out_path)


if __name__ == '__main__':

    date = datetime.strptime('06/04/2020', '%d/%m/%Y').date()
    luigi.build([QnFinal(date=date)])
