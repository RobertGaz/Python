import datetime as dt

from numpy import array, repeat, double, max as np_max
from pandas import read_excel, isnull, merge, Series, concat


buy_init_list = ['Part Number',
    'Smart Account Mandatory', 
     'Description', 
     'Service Duration (Months)', 
     'Estimated Lead Time (Days)', 
     'Unit List Price', 
     'Pricing Term', 
     'Qty', 

     'Unit Net Price', 
     'List Price',
     'ListPrice',

     'Disc(%)', 
     'Extended Net Price']

def columns_info_line(df):
    for i in range(1, df.shape[0]):
        a = array(df.iloc[i])
        for j in range(0, len(a)):
            if a[j] in buy_init_list:
                return i
              
                            
def part_number_column(df, line):
    columns = df.iloc[line]
    for j in range(len(columns)):
        col = columns[j]
        if columns[j] == 'Part Number':
            return j


def init_buy():
    df = read_excel(buy_filename)
    col_info_line = columns_info_line(df)
    part_num_col = part_number_column(df, col_info_line)
    
    startline = col_info_line + 1
    
    while isnull(df.iloc[startline]).sum() > (isnull(df.iloc[startline]) == False).sum():
        startline +=1
        
    i = startline
    
    while True:
        part_num = df.iloc[i, part_num_col]    
        if isnull(part_num):
            break
        i +=1
    endline = i
    

    buy = df.iloc[startline:endline]
    buy.columns = list(df.iloc[col_info_line])
    buy = buy.reset_index(drop=True)
    
    return buy

# Transition Period
# All Architecture SKUs
# Enterprise Networks
# Collaboration
# Security
# Data Center
# Service Provider Technology
# Meraki

def open_table_in_sku(name):
    df = read_excel(sku_filename, sheet_name=name)
    new = df.iloc[1:len(df)]
    new.columns = df.iloc[0]
    new = new.reset_index(drop=True)
    return new

def init_sku():
    tp = open_table_in_sku('Transition Period')
#     aas = open_table_in_sku('All Architecture SKUs')
    en = open_table_in_sku('Enterprise Networks')
    c = open_table_in_sku('Collaboration')
    s = open_table_in_sku('Security')
    dc = open_table_in_sku('Data Center')
    spt = open_table_in_sku('Service Provider Technology')
    m = open_table_in_sku('Meraki')
    return tp, en, c, s, dc, spt, m

def df_pieces(buy, tp, en, c, s, dc, spt, m):
    b_buy = buy[['Part Number', 'Extended Net Price']]
    b_buy = b_buy.rename(columns={'Part Number': 'pid'})

    tp = tp[['Number', 'Rebate %', 'Start', 'End',  'Y/N?', 'Period Rebate %', 'Start Date', 'End Date']]
    tp = tp.rename(columns={'Number': 'pid', 'Rebate %': 'tp_rebate', 'Start': 'tp_start', 'End': 'tp_end', 
                            'Period Rebate %': 'tp_after_rebate', 'Start Date': 'tp_after_start', 'End Date': 'tp_after_end', 
                            'Y/N?': 'delete'})
    tp.delete = tp.delete == 'Y' 
    

#     aas = aas[['PID', 'Payout']]
#     aas = aas.rename(columns={'PID': 'pid', 'Payout': 'aas_rebate'})

    en = en[['PID', 'Payout']]
    en = en.rename(columns={'PID': 'pid', 'Payout': 'en_rebate'})

    c = c[['PID', 'Payout']]
    c = c.rename(columns={'PID': 'pid', 'Payout': 'c_rebate'})

    s = s[['PID', 'Payout']]
    s = s.rename(columns={'PID': 'pid', 'Payout': 's_rebate'})

    dc = dc[['PID', 'Payout']]
    dc = dc.rename(columns={'PID': 'pid', 'Payout': 'dc_rebate'})

    spt = spt[['PID', 'Payout']]
    spt = spt.rename(columns={'PID': 'pid', 'Payout': 'spt_rebate'})

    m = m[['PID', 'Payout']]
    m = m.rename(columns={'PID': 'pid', 'Payout': 'm_rebate'})
    
    return b_buy, tp, en, c, s, dc, spt, m

def rebate_table(b_buy, tp, en, c, s, dc, spt, m):
    
    tab = merge(b_buy, tp, on='pid', how='left')

#     tab = merge(tab, aas, on='pid', how='left')
    tab = merge(tab, en, on='pid', how='left')
    tab = merge(tab, c, on='pid', how='left')
    tab = merge(tab, s, on='pid', how='left')
    tab = merge(tab, dc, on='pid', how='left')
    tab = merge(tab, spt, on='pid', how='left')
    tab = merge(tab, m, on='pid', how='left')

    tab.tp_rebate.fillna(0, inplace=True)
    tab.c_rebate.fillna(0, inplace=True)
#     tab.aas_rebate.fillna(0, inplace=True)
    tab.en_rebate.fillna(0, inplace=True)
    tab.c_rebate.fillna(0, inplace=True)
    tab.s_rebate.fillna(0, inplace=True)
    tab.dc_rebate.fillna(0, inplace=True)
    tab.spt_rebate.fillna(0, inplace=True)
    tab.m_rebate.fillna(0, inplace=True)



    tab.delete = tab.delete == True

    tab['rebate1'] = repeat(None, len(tab))
    tab['start1'] = repeat(None, len(tab))
    tab['end1'] = repeat(None, len(tab))

    tab['rebate2'] = repeat(None, len(tab))
    tab['start2'] = repeat(None, len(tab))
    tab['end2'] = repeat(None, len(tab))

    now = dt.datetime.now() 

    during_tp = (now > tab.tp_start) & (now <  tab.tp_end + dt.timedelta(days=1))
    after_tp = (now > tab.tp_after_start) & (now < tab.tp_after_end + dt.timedelta(days=1)) 
    
    if during_tp.max():
    
        tab.loc[during_tp, 'start1'] = tab.tp_start[during_tp].dt.strftime('%d.%m.%Y')
        tab.loc[during_tp, 'end1'] = tab.tp_end[during_tp].dt.strftime('%d.%m.%Y')


        tab.loc[during_tp & (tab.delete == False), 'start2'] = tab.tp_after_start[during_tp].dt.strftime('%d.%m.%Y')
        tab.loc[during_tp & (tab.delete == False), 'end2'] = tab.tp_after_end[during_tp].dt.strftime('%d.%m.%Y')

        tab.loc[during_tp, 'rebate1'] = tab.tp_rebate[during_tp]
        tab.loc[during_tp, 'rebate2'] = tab.tp_after_rebate[during_tp]

    if after_tp.max():
        tab.loc[after_tp, 'start1'] = tab.tp_after_start[after_tp].dt.strftime('%d.%m.%Y')
        tab.loc[after_tp, 'end1'] = tab.tp_after_end[after_tp].dt.strftime('%d.%m.%Y')
        tab.loc[after_tp, 'rebate1'] = tab.tp_after_rebate[after_tp]


    no_tp = isnull(tab.rebate1)

    tab.loc[no_tp, 'rebate1'] = np_max(tab.loc[no_tp, ['en_rebate', 'c_rebate', 's_rebate', \
                                                       'dc_rebate', 'spt_rebate', 'm_rebate']], axis=1)

    tab = tab.rename(columns={'rebate1': 'Rebate', 'start1': 'Start', 'end1': 'End', \
                              'rebate2': 'Next Rebate', 'start2': 'Next Start', 'end2': 'Next End'})

    # tab['Unit Rebate'] = tab['Unit Net Price'] * tab['Rebate']
    tab['Extended Rebate'] = tab['Extended Net Price'] * tab['Rebate']
    # tab['Next Unit Rebate'] = tab['Unit Net Price'] * tab['Next Rebate']
    tab['Next Extended Rebate'] = tab['Extended Net Price'] * tab['Next Rebate']

    tab['Rebate'] = tab['Rebate'] * 100
    tab['Next Rebate'] = tab['Next Rebate'] * 100

    tab.loc[during_tp & tab.delete, 'Next Rebate'] = 'Удаляется из Rebate' 

    tab = tab.rename(columns={'Rebate': 'Rebate (%)', 'Next Rebate': 'Next Rebate (%)'})

    rebate_tab = tab[['Rebate (%)', 'Extended Rebate', 'Start', 'End',
                      'Next Rebate (%)', 'Next Extended Rebate', 'Next Start', 'Next End']]
    return rebate_tab    

def totals_bottom(tab):
    totals = Series({'Disc(%)': 'Product Total:',
            'Extended Net Price': tab['Extended Net Price'].sum(), 
            'Rebate (%)': 'Total Rebate:',
            'Extended Rebate': tab['Extended Rebate'].sum(),
            'Next Rebate (%)': 'Total Next Rebate',
            'Next Extended Rebate': tab['Next Extended Rebate'].sum()})
    totals.name = len(tab)
    
    return totals

def process():
    buy = init_buy()
    tp, en, c, s, dc, spt, m = init_sku()
    b_buy, tp, en, c, s, dc, spt, m = df_pieces(buy, tp, en, c, s, dc, spt, m)
    rebate_tab = rebate_table(b_buy, tp, en, c, s, dc, spt, m)

    res = concat([buy, rebate_tab], axis=1)
    totals = totals_bottom(res)
    res = res.append(totals)

#     res['Unit Rebate'] = res['Unit Rebate'].astype(double).round(2)
    res['Extended Rebate'] = res['Extended Rebate'].astype(double).round(2)
#     res['Next Unit Rebate'] = res['Next Unit Rebate'].astype(double).round(2)
    res['Next Extended Rebate'] = res['Next Extended Rebate'].astype(double).round(2)

    res.to_excel(out_filename, index=False)
    return res

#######################################################################################################################


from tkinter import *
from tkinter import filedialog as fd
import webbrowser

space = '                                          '

buy_filename = ''
sku_filename = ''
out_filename = ''

def get_sku():
    global sku_filename
    sku_filename = fd.askopenfilename(filetypes=(("Excel files", "*.xls;*.xlsx"), ("All files", "*.*")) )
    Label(text=sku_filename).grid(row=0, column=1, columnspan=3)

def get_buy():
    global buy_filename
    buy_filename = fd.askopenfilename(filetypes=(("Excel files", "*.xls;*.xlsx"), ("All files", "*.*")) )
    Label(text=buy_filename).grid(row=1, column=1, columnspan=3)
    
    make_out_filename()
    Label().grid(row=2, column=1, columnspan=3, ipadx=140)
    Label(text=out_filename).grid(row=2, column=1, columnspan=3) 
        
        
def make_out_filename():
    global out_filename
    out_filename = buy_filename.rsplit('/', 1)[0]+'/OUT-'+buy_filename.rsplit('/', 1)[1]
    
def get_out():
    global out_filename
    if out_filename == '':
        out_filename = fd.asksaveasfilename(filetypes=(("Excel files", "*.xls;*.xlsx"), ("All files", "*.*")), 
                                            defaultextension='.xlsx', confirmoverwrite=True)
    else:    
        out_filename = fd.asksaveasfilename(filetypes=(("Excel files", "*.xls;*.xlsx"), ("All files", "*.*")),
                                            initialdir=out_filename.rsplit('/', 1)[0],
                                            initialfile='OUT-'+out_filename.rsplit('/', 1)[1],
                                            defaultextension='.xlsx',
                                            confirmoverwrite=True)
    Label().grid(row=2, column=1, columnspan=3, ipadx=140)    
    Label(text=out_filename).grid(row=2, column=1, columnspan=3)

    

    
def process_wrapper():
    res = process()
    if need_out.get():
        res.fillna('').to_html(out_filename.rsplit('/', 1)[0] + '/out.html')
        webbrowser.open('file://' + out_filename.rsplit('/', 1)[0] + '/out.html')
    
root = Tk(className='RebateCalc')

Label(text='Таблица SKU:').grid(row=0, column=0, sticky=W, padx=10, pady=10)
Label().grid(row=0, column=1, columnspan=3, ipadx=120, pady=10)
Button(text='Файл...', command=get_sku).grid(row=0, column=4, sticky=E, ipadx=7, padx=10, pady=10)

Label(text='Таблица с закупкой:').grid(row=1, column=0, sticky=W, padx=10, pady=6)
Label().grid(row=1, column=1, columnspan=3, ipadx=120, pady=10)
Button(text='Файл...', command=get_buy).grid(row=1, column=4, sticky=E, ipadx=7, padx=10, pady=10)

Label(text='Таблица с результатом:').grid(row=2, column=0, sticky=W, padx=10, pady=10)
Label().grid(row=2, column=1, columnspan=3, ipadx=120, pady=10)
Button(text='Файл...', command=get_out).grid(row=2, column=4, sticky=E, ipadx=7, padx=10, pady=10)

need_out = BooleanVar()
need_out.set(1)
Checkbutton(text="Показать результат сразу", 
            variable=need_out, onvalue=1, offvalue=0).grid(row=3, column=0, sticky=W, padx=10, pady=10)

Button(text='Поехали!', command=process_wrapper).grid(row=5, column=1, ipadx=7, padx=10, pady=10)


root.mainloop()
