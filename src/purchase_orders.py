import luigi
import os
import pandas as pd
import json
import re
from datetime import timedelta, datetime


def year_week(date):
    return f'{date.year}-{date.isocalendar()[1]}'


class PurchaseOrdersConfig(luigi.Config):
    comment_patterns = luigi.DictParameter()


class PurchaseOrders(luigi.Task):
    '''
    Read purchase orders (PO). Each line in the source file is a
    single PO-Position-Event.
        - PO: Purchase Order.
        - Position: a Part Number (PN) being purchased with some quantity
        - Event: a delivery event, defined by some quantity of the PN
        scheduled to be delivered at some date.
    Lines which are already delivered are discarded.
    '''
    date = luigi.DateParameter()

    def output(self):
        file = f'data/output/{year_week(self.date)}/purchase_orders.csv'
        return luigi.LocalTarget(file)

    def run(self):
        '''
        Read PO's, rename and discard delivered
        '''
        file = f'data/input/{year_week(self.date)}/zmm_cons_repartos.xlsx'
        rename = {
            'Documento compras': 'po',
            'Posición': 'pos',
            'Reparto': 'ev',
            'Material': 'pn',
            'Grupo de compras': 'prchs_grp',
            'Elemento PEP': 'pep',
            'Proveedor': 'id',
            'Nombre 1': 'supplier',
            'Fecha de entrega': 'date_del',
            'Fecha entrega estad.': 'date_stat',
            'Cantidad de reparto': 'qty',
            'Ctd.EM Reparto': 'qty_del',
            'Indicador de acuse de pedido': 'ack',
            'Precio neto pedido': 'net_price',
            'Entrega final': 'final_delivery',
            'Fecha albarán': 'date_grd',
            'Centro': 'center',
            'Texto breve de acuse de pedido': 'ack_text',
            'Solicitud de pedido': 'pr',
            'Centro': 'center'
        }
        usecols = rename.keys()
        df = pd.read_excel(file, usecols=usecols)
        df = df.rename(columns=rename)
        df['delivered'] = df.apply(self.is_delivered, axis=1)
        df = df[df['delivered'] == False]
        with self.output().temporary_path() as temp_out_path:
            df.to_csv(temp_out_path, index=False)

    def is_delivered(self, row):
        '''
        A row is delivered when:
            - There is a goods receipt date (date_grd)
            - The 'final_delivery' flag is set
            - Expected quantity is equal to delivered quantity
        '''
        if pd.notnull(row['date_grd']):
            return True
        elif pd.notnull(row['final_delivery']):
            return True
        elif row['qty'] == row['qty_del']:
            return True
        else:
            return False


class Comments(luigi.Task):
    '''
    Read Purchase Order comments from buyer files. Each buyer is provided with
    a file with their assigned PO's, and must provide updated comments about
    the status. This comments must follow some rules.
    '''
    date = luigi.DateParameter()

    def output(self):
        file = f'data/output/{year_week(self.date)}/comments.csv'
        return luigi.LocalTarget(file)

    def run(self):
        '''
        Buyers write commments in the files from previous weeks. We need to retrieve
        all these comments to match them with the current PO information from SAP.
        '''
        path = f'data/input/{year_week(self.date)}/{year_week(self.date - timedelta(days=7))}'
        rename = {
            'PO': 'po',
            'Pos': 'pos',
            'Ev': 'ev',
            'OBS SOI': 'comments'
        }
        usecols = rename.keys()
        dfs = []

        # Compile all comments
        for f in os.listdir(path):
            if '.xlsx' in f:
                df = pd.read_excel(os.path.join(path, f), usecols=usecols)
                dfs.append(df)
        dfs = pd.concat(dfs)
        dfs = dfs.rename(columns=rename)

        # Validate comments
        dfs['comment_valid'] = dfs['comments'].apply(self.validate_comment)
        with self.output().temporary_path() as temp_out_path:
            dfs.to_csv(temp_out_path, index=False)

    def validate_comment(self, comment):
        '''
        Comments are validated using reg expressions defined in luigi.cfg.
        Each reg exp is defined as a dict:
            - name: designation of the type of comment
            - pattern: reg exp pattern
            - validity: number of days in which the comment is valid
        The comment date is the first capturing group in the reg exp pattern,
        and this is compared with the date parameter.
        '''
        if pd.isnull(comment):
            return False
        else:
            for p in cfg.comment_patterns:
                m = re.match(p['pattern'], comment)
                if m:
                    comment_date = datetime.strptime(m[1], '%d/%m/%Y').date()
                    validity = timedelta(days=p['validity'])
                    return comment_date + validity <= self.date
        return False


class PoComments(luigi.Task):
    '''
    Match PO's and comments
    '''
    date = luigi.DateParameter()

    def requires(self):
        return [
            PurchaseOrders(date=self.date),
            Comments(date=self.date)
        ]

    def output(self):
        file = f'data/output/{year_week(self.date)}/po_comments.csv'
        return luigi.LocalTarget(file)

    def run(self):
        index = ['po', 'pos', 'ev']
        po = pd.read_csv(self.input()[0].path, index_col=index)
        comments = pd.read_csv(self.input()[1].path, index_col=index)
        df = po.join(comments, how='left')
        with self.output().temporary_path() as temp_out_path:
            df.to_csv(temp_out_path)


if __name__ == '__main__':

    # Config
    cfg = PurchaseOrdersConfig()
    date = datetime.strptime('06/04/2020', '%d/%m/%Y').date()
    luigi.build([PoComments(date=date)])
