from datetime import date
from lib.data_service import DataService
from lib.sharepoint_uploader import SharePointUploader
from lib.emailer import Emailer
from lib.OneSharpBalances import OneSharpBalances
from lib.ColchisInterests import ColchisInterests
import sys

if len(sys.argv) < 2:
    print('Usage python upload_sharepoint [report name]')
    exit()
    
# report name like Colchis Interest, OneSharp Balances etc.
filename = sys.argv[1] 
report_key = filename.replace(' ', '').lower()

#change algos to key: [lambda, where-clause] if different where-clauses are needed
algos = {
    'onesharpbalances': lambda: OneSharpBalances(),
    'colchisinterests': lambda: ColchisInterests()
}

if report_key not in algos:
    print('Report name could not be found. Please check your spelling')
    exit()

report = algos[report_key]()

try:
    uploader = SharePointUploader(filename+'_'+str(date.today())+'.csv', report.get_report_bytes())
    uploader.upload_file()
    #emailer = Emailer('servicing@wcp.team')
    #emailer.send_email()
except Execption as e:  
    emailer = Emailer('tdetwiler@wcp.team', 'Colchis Interest Report Failed', 'Please check the reason')
    emailer.send_email()