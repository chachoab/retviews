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

# Config
cfg = PurchaseOrdersConfig()

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
            'Proveedor': 'supplier_id',
            'Nombre 1': 'supplier',
            'Fecha documento': 'date_doc',
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
    the status.
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
        path = f'data/input/{year_week(self.date)}/comments'
        rename = {
            'PO': 'po',
            'Pos': 'pos',
            'Ev': 'ev',
            'OBS SOI': 'comment'
        }
        usecols = rename.keys()
        dfs = []

        # Compile all comments
        for f in os.listdir(path):
            if '.xlsx' in f:
                try:
                    df = pd.read_excel(os.path.join(path, f), usecols=usecols)
                except Exception as e:
                    # TODO notify
                    pass
                dfs.append(df)
        dfs = pd.concat(dfs)
        dfs = dfs.rename(columns=rename)

        with self.output().temporary_path() as temp_out_path:
            dfs.to_csv(temp_out_path, index=False)


class PoComments(luigi.Task):
    '''
    Join PO's and comments and validate.

    Comments are validated using reg expressions defined in luigi.cfg. If the PO
    was created less than a month ago, the comment (or lack of) is validated).

    Each reg exp is defined as a dict:

        - name: designation of the type of comment
        - pattern: reg exp pattern
        - validity: number of days in which the comment is valid

    The comment date is the first capturing group in the reg exp pattern,
    and this is compared with the date parameter.
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
        # Join PO's and comments
        po = pd.read_csv(self.input()[0].path, index_col=index, parse_dates=['date_doc'])
        comments = pd.read_csv(self.input()[1].path, index_col=index)
        df = po.join(comments, how='left')

        #Validate comments
        df['comment_valid'] = df.apply(self.validate_comment, axis=1)
        with self.output().temporary_path() as temp_out_path:
            df.to_csv(temp_out_path)

    def validate_comment(self, row):
        if row['date_doc'] >= pd.Timestamp(self.date - timedelta(days=30)):
            return True
        elif pd.isnull(row['comment']):
            return False
        else:
            for p in cfg.comment_patterns:
                m = re.match(p['pattern'], row['comment'])
                if m:
                    try:  # dates are input manually
                        comment_date = datetime.strptime(m[1], '%d/%m/%Y').date()
                        validity = timedelta(days=p['validity'])
                        return comment_date + validity <= self.date
                    except ValueError as e:
                        # TODO log errors
                        return False
        return False

class PoFinal(luigi.Task):
    '''
    Join PO's with program, area and buyer information.
    
    Program is obtained from PEP, and relates to the product family which the
    PN belongs to.

    Area and buyer are obtained from Supplier ID, and indicate the department
    and person responsible for a PO.
    '''
    date = luigi.DateParameter()

    def requires(self):
        return [
            PoComments(date=self.date),
        ]

    def output(self):
        file = f'data/output/{year_week(self.date)}/po_final.csv'
        return luigi.LocalTarget(file)

    def run(self):
        # Join PO's and program info
        po = pd.read_csv(self.input()[0].path)
        pep = pd.read_csv('data/input/pep.csv')
        df = po.merge(pep, on='pep', how='left')
        df['pep'] = df['pep'].fillna('Others')

        # Join PO and supplier info
        supplier = pd.read_csv('data/input/supplier.csv')
        df = df.merge(supplier, on='supplier_id', how='left')
        df = df.fillna({'buyer': 'external', 'area': 'external'})

        #Validate comments
        with self.output().temporary_path() as temp_out_path:
            df.to_csv(temp_out_path)


if __name__ == '__main__':

    date = datetime.strptime('06/04/2020', '%d/%m/%Y').date()
    luigi.build([PoFinal(date=date)])
