import urllib
import pandas as pd
import sqlite3
import csv
import os
import time
#StockPrice : https://www.alphavantage.co/documentation/


def getStickerURL_FULL(sticker):
    #return JSON Output
    alphavantageKey = 'CZM4SAYSZXRXLYGK'
    URL='https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol=%s&outputsize=full&apikey=%s&datatype=csv'%(sticker,alphavantageKey)
    return URL

def getStickerURL_COMPACT(sticker):
    #return JSON Output
    alphavantageKey = 'CZM4SAYSZXRXLYGK'
    URL='https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol=%s&outputsize=compact&apikey=%s&datatype=csv'%(sticker,alphavantageKey)
    return URL

def getJsonSticker(sticker):
    #return a Dict
    URL = getStickerURL_FULL(sticker)
    #req = urllib.request.Request(URL)
    #r = urllib.request.urlopen(req).read()
    #content = json.loads(r.decode('utf-8'))

    ftpstream = urllib.request.urlopen(URL)
    content = csv.reader(ftpstream)  # with the appropriate encoding 
    return content

def parsingDict(sticker):
    #return DataFrame
##    dictKey = "Time Series (Daily)"
    #dp = pd.DataFrame.from_dict(content[dictKey])
    URL = getStickerURL_FULL(sticker)
    dp = pd.read_csv(URL)
    return dp

def connectDB(dp,db):
    #create new Databases if not exists
    
    databasePath = "./Databases"
    if not os.path.exists(databasePath):
        os.mkdir(databasePath)

    dbName = databasePath + "/" + db + ".db"
    try:
        conn = sqlite3.connect(dbName)
##        print(sqlite3.version)
    except Error as e:
        print(e)
    finally:
        conn.close()
    return dbName

def createDBSchema(dp,db,command):
    dbName = connectDB(dp,db)
    conn = sqlite3.connect(dbName)
    conn.execute(command)
    conn.commit()
    conn.close()
    
def populateDB(df,tb_name):
    #http://yznotes.com/write-pandas-dataframe-to-sqlite/
    dbName = connectDB(df,tb_name)
    conn=sqlite3.connect(dbName)
    cur = conn.cursor()                                 
    wildcards = ','.join(['?'] * len(df.columns))
    data = [tuple(x) for x in df.values]
    
    #create new table if not exists

    col_str = '"' + '","'.join(df.columns) + '"'
    cur.execute("create table IF NOT EXISTS %s (%s,UNIQUE ('timestamp','StockName'))" % (tb_name, col_str))
    
    for entry in data:
        try:
    ##        cur.executemany("insert into %s values(%s)" % (tb_name,wildcards), data)
            cur.execute("insert into %s values(%s)" % (tb_name,wildcards), entry)
        except sqlite3.IntegrityError:
##            conn.commit()
            print ('#####')
            print (entry)
            print ('Stop importing')
            break
        except sqlite3.OperationalError:
            print ("Failed to Import a Stock")
            print (data)
            print ("####################")
            break
    conn.commit()
    conn.close()
    return data
    
def main(sticker):
    df = parsingDict(sticker)
    df['StockName']=sticker
    #create Table if not Exists
    tb_name = "StockPrices"
##    cmdCreateTable = 'CREATE TABLE IF NOT EXISTS %s (timestamp TEXT PRIMARY KEY ASC,Stock TEXT SECONDARY KEY ASC, open DOUBLE, high DOUBLE, low DOUBLE,close DOUBLE,\
##    adjusted_close DOUBLE, volume DOUBLE, dividend_amount DOUBLE, split_coefficient DOUBLE);'%tb_name
    
##    createDBSchema(df,tb_name,cmdCreateTable)
    return populateDB(df,tb_name)

##
##sticker='MSFT'
##ans = main(sticker)

#download 5 stocks per a minute limitation in the API call

def getSP500(name):
#   S&P 500
    stickers=['AAPL','AMZN','GOOGL','GOOG','MSFT','FB','BRK.B','JPM','XOM','JNJ','V','BAC','WFC','INTC','WMT','UNH','CVX','HD','PFE','BA','MA','CSCO','T','VZ','PG','ORCL','KO','NFLX','C','MRK','DIS','NVDA','ABBV','DWDP','CMCSA','PEP','IBM','MCD','PM','ADBE','AMGN','NKE','MMM','MDT','AVGO','GE','TXN','UNP','HON','ABT','ACN','MO','BKNG','CRM','UTX','PYPL','UPS','LLY','SLB','GILD','COST','MS','CAT','BMY','QCOM','LMT','GS','TMO','AXP','USB','BLK','FOXA','FOX','LOW','SBUX','TWX','COP','SCHW','NEE','KHC','DHR','CVS','FDX','CHTR','PNC','MU','EOG','WBA','BIIB','OXY','SYK','ANTM','BDX','AET','GM','ADP','AMT','CB','MDLZ','TJX','ATVI','AGN','CSX','GD','EL','RTN','CME','BK','CELG','CL','NOC','ISRG','INTU','PSX','SPGI','DUK','SPG','ITW','AMAT','VLO','AIG','MAR','DE','F','COF','CTSH','MET','CCL','PX','ESRX','EMR','BSX','LYB','EA','SO','STZ','NSC','D','ZTS','ICE','CI','ILMN','ECL','HUM','CCI','MMC','PRU','TGT','HAL','BBT','BAX','EXC','VRTX','DAL','EBAY','HPQ','ADI','SHW','PSA','HCA','KMI','WM','PGR','BHGE','KMB','APD','STT','FIS','AFL','APC','TRV','SYY','TEL','ETN','TWTR','AON','PLD','REGN','MPC','MCO','VFC','ALL','JCI','EW','ROST','AEP','STI','MNST','EQIX','PXD','FISV','RHT','MCK','LUV','ADSK','TROW','SRE','ROP','LRCX','ALGN','FTV','WY','OKE','APH','APTV','ALXN','YUM','SYF','PEG','TSN','PPG','DG','DFS','GIS','ADM','BF.B','CNC','MTB','HLT','PAYX','FCX','RCL','GLW','HPE','WDC','NTRS','MCHP','DXC','CMI','EQR','AVB','K','ORLY','ZBH','IP','RSG','ED','IR','PH','COL','PCAR','ROK','KR','WMB','XEL','SWK','DPS','DLR','KEY','WELL','IQV','AMP','CBS','NUE','A','DVN','FITB','DLTR','BBY','RF','ABC','NTAP','PCG','ANDV','CERN','UAL','NEM','WLTW','CTAS','INFO','CFG','MYL','AAL','GGP','IDXX','ABMD','EIX','HSY','CTL','LH','HRL','VTR','HIG','WEC','PPL','WYNN','GPN','CXO','SWKS','EXPE','MSI','BXP','BEN','AZO','VRSK','HES','SBAC','GWW','TDG','XLNX','HRS','DTE','KLAC','VMC','AME','MGM','LEN','ES','TXT','OMC','MRO','CAH','ETFC','TIF','HBAN','STX','SIVB','CLX','FE','CBRE','NBL','CMA','DISH','DHI','PFG','L','APA','MHK','NDAQ','NOV','AMD','HST','WAT','TSS','INCY','EMN','CA','LLL','ESS','MSCI','WRK','FAST','CAG','DGX','RMD','O','MTD','ANSS','ULTA','EFX','EQT','LNC','TAP','XL','CTXS','MLM','AWK','FTI','JBHT','RJF','DISCA','ETR','AKAM','MKC','GPC','TTWO','AEE','VRSN','VNO','SNPS','URI','EXPD','TPR','DISCK','SYMC','ARE','IPGP','KMX','ADS','CMG','XYL','DVA','BLL','CDNS','NWL','IT','CHRW','CMS','PVH','KSS','NCLH','CHD','GPS','EXR','FMC','SJM','AJG','DOV','MAS','VIAB','M','DRI','COO','CBOE','GRMN','HAS','RL','CINF','IVZ','KSU','HSIC','HCP','PKG','NLSN','VAR','CNP','MOS','CPB','FFIV','HOLX','MAA','UHS','ZION','COTY','QRVO','WHR','AOS','COG','NRG','CF','PRGO','LKQ','ALB','UAA','XRAY','AAP','DRE','BWA','KORS','REG','LB','IFF','NKTR','UDR','HII','TMK','UA','WU','JNPR','IRM','NWS','AVY','NWSA','RE','LNT','JEC','SNA','IPG','TSCO','SLG','FRT','PHM','UNM','AES','RHI','PKI','PNW','ARNC','AMG','JWN','FBHS','NI','XEC','TRIP','MAC','PNR','ALLE','ALK','JEF','HOG','FLIR','EVRG','HBI','SEE','KIM','XRX','HP','FLR','FL','PBCT','AIV','MAT','GT','LEG','NFX','SRCL','EVHC','BHF','FLS','PWR','SCG','AIZ','AYI','HRB','RRC']
    sName=['Apple','Amazon.com','Alphabet','Alphabet','Microsoft','Facebook','Berkshire Hathaway','JPMorgan Chase','Exxon Mobil','Johnson & Johnson','Visa','Bank of America','Wells Fargo','Intel','Walmart','UnitedHealth Group','Chevron','Home Depot','Pfizer','Boeing','Mastercard','Cisco Systems','AT&T','Verizon Communications','Procter & Gamble','Oracle','Coca-Cola','Netflix','Citigroup','Merck & Co','Walt Disney','NVIDIA','AbbVie','DowDuPont','Comcast','PepsiCo','IBM','McDonalds','Philip Morris Intl','Adobe Systems','Amgen','Nike','3M','Medtronic','Broadcom','General Electric','Texas Instruments','Union Pacific','Honeywell International','Abbott Laboratories','Accenture','Altria Group','Booking Holdings','Salesforce.com','United Technologies','PayPal Holdings','United Parcel Service','Eli Lilly','Schlumberger','Gilead Sciences','Costco Wholesale','Morgan Stanley','Caterpillar','Bristol-Myers Squibb','Qualcomm','Lockheed Martin','Goldman Sachs Group','Thermo Fisher Scientific','American Express','US Bancorp','BlackRock','Twenty-First Century Fox','Twenty-First Century Fox','Lowes Companies','Starbucks','Time Warner','ConocoPhillips','Charles Schwab','NextEra Energy','Kraft Heinz','Danaher','CVS Health','FedEx','Charter Communications','PNC Financial Services Gr','Micron Technology','EOG Resources','Walgreens Boots Alliance','Biogen','Occidental Petroleum','Stryker','Anthem','Becton, Dickinson and Co','Aetna','General Motors','Automatic Data Processing','American Tower','Chubb','Mondelez International','TJX Companies','Activision Blizzard','Allergan','CSX','General Dynamics','The Estee Lauder','Raytheon','CME Group','Bank of New York Mellon','Celgene','Colgate-Palmolive','Northrop Grumman','Intuitive Surgical','Intuit','Phillips 66','S&P Global','Duke Energy','Simon Property Group','Illinois Tool Works','Applied Materials','Valero Energy','American International Gr','Marriott International','Deere','Ford Motor','Capital One Financial','Cognizant Tech Solns','MetLife','Carnival','Praxair','Express Scripts Holding','Emerson Electric','Boston Scientific','LyondellBasell Industries','Electronic Arts','Southern','Constellation Brands','Norfolk Southern','Dominion Energy','Zoetis','Intercontinental Exchange','Cigna','Illumina','Ecolab','Humana','Crown Castle Intl','Marsh & McLennan','Prudential Financial','Target','Halliburton','BB&T','Baxter International','Exelon','Vertex Pharmaceuticals','Delta Air Lines','eBay','HP','Analog Devices','Sherwin-Williams','Public Storage','HCA Healthcare','Kinder Morgan','Waste Management','Progressive','Baker Hughes, a GE','Kimberly-Clark','Air Products & Chemicals','State Street Corporation','Fidelity National Info','Aflac','Anadarko Petroleum','Travelers Companies','Sysco','TE Connectivity','Eaton','Twitter','Aon','Prologis','Regeneron Pharmaceuticals','Marathon Petroleum','Moodys','VF','Allstate','Johnson Controls','Edwards Lifesciences','Ross Stores','American Electric Power','SunTrust Banks','Monster Beverage','Equinix','Pioneer Natural Resources','Fiserv','Red Hat','McKesson','Southwest Airlines','Autodesk','T. Rowe Price Group','Sempra Energy','Roper Technologies','Lam Research','Align Technology','Fortive','Weyerhaeuser','ONEOK','Amphenol','Aptiv','Alexion Pharmaceuticals','Yum Brands','Synchrony Financial','Public Service Enterprise','Tyson Foods','PPG Industries','Dollar General','Discover Financial','General Mills','Archer-Daniels Midland','Brown-Forman','Centene','M&T Bank','Hilton Worldwide Holdings','Paychex','Freeport-McMoRan','Royal Caribbean Cruises','Corning','Hewlett Packard Enterprise','Western Digital','Northern Trust','Microchip Technology','DXC Technology','Cummins','Equity Residential','AvalonBay Communities','Kellogg','OReilly Automotive','Zimmer Biomet Holdings','International Paper','Republic Services','Consolidated Edison','Ingersoll-Rand','Parker Hannifin','Rockwell Collins','PACCAR','Rockwell Automation','Kroger','Williams Companies','Xcel Energy','Stanley Black & Decker','Dr Pepper Snapple Group','Digital Realty Trust','KeyCorp','Welltower','IQVIA Holdings','Ameriprise Financial','CBS','Nucor','Agilent Technologies','Devon Energy','Fifth Third Bancorp','Dollar Tree Stores','Best Buy Co','Regions Financial','AmerisourceBergen','NetApp','PG&E','Andeavor','Cerner','United Continental Holdings','Newmont Mining','Willis Towers Watson','Cintas','IHS Markit','Citizens Financial Group','Mylan','American Airlines Group','GGP','IDEXX Laboratories','Abiomed','Edison International','The Hershey','CenturyLink','Laboratory Corp','Hormel Foods','Ventas','Hartford Financial','WEC Energy Group','PPL','Wynn Resorts','Global Payments','Concho Resources','Skyworks Solutions','Expedia Group','Motorola Solutions','Boston Properties','Franklin Resources','AutoZone','Verisk Analytics','Hess','SBA Communications','W.W. Grainger','TransDigm Group','Xilinx','Harris','DTE Energy','KLA-Tencor','Vulcan Materials','AMETEK','MGM Resorts International','Lennar','Eversource Energy','Textron','Omnicom Group','Marathon Oil','Cardinal Health','E*TRADE Financial','Tiffany','Huntington Bancshares','Seagate Technology','SVB Financial','Clorox','FirstEnergy','CBRE Group','Noble Energy','Comerica','DISH Network','D.R. Horton','Principal Financial Group','Loews','Apache','Mohawk Industries','Nasdaq','National Oilwell Varco','Advanced Micro Devices','Host Hotels & Resorts','Waters','Total System Services','Incyte','Eastman Chemical','CA','L3 Technologies','Essex Property Trust','MSCI','WestRock','Fastenal','Conagra Brands','Quest Diagnostics','ResMed','Realty Income','Mettler-Toledo Intl','Ansys','Ulta Beauty','Equifax','EQT','Lincoln National','Molson Coors Brewing','XL Group','Citrix Systems','Martin Marietta Materials','American Water Works Co','TechnipFMC','JB Hunt Transport','Raymond James Financial','Discovery','Entergy','Akamai Technologies','McCormick & Co','Genuine Parts','Take-Two Interactive','Ameren','VeriSign','Vornado Realty Trust','Synopsys','United Rentals','Expeditors International','Tapestry','Discovery','Symantec','Alexandria Real Estate','IPG Photonics','CarMax','Alliance Data Systems','Chipotle Mexican Grill','Xylem','DaVita','Ball','Cadence Design Systems','Newell Brands','Gartner','C.H. Robinson Worldwide','CMS Energy','PVH','Kohls','Norwegian Cruise Line','Church & Dwight Co','Gap','Extra Space Storage','FMC','JM Smucker','Arthur J. Gallagher','Dover','Masco','Viacom','Macys','Darden Restaurants','The Cooper Companies','Cboe Global Markets','Garmin','Hasbro','Ralph Lauren','Cincinnati Financial','Invesco','Kansas City Southern','Henry Schein','HCP','Packaging Corp of America','Nielsen Holdings','Varian Medical Systems','CenterPoint Energy','Mosaic','Campbell Soup','F5 Networks','Hologic','Mid-America Apartment','Universal Health Services','Zions Bancorp','Coty','Qorvo','Whirlpool','A.O. Smith','Cabot Oil & Gas','NRG Energy','CF Industries Holdings','Perrigo Co','LKQ','Albemarle','Under Armour','Dentsply Sirona','Advance Auto Parts','Duke Realty','BorgWarner','Michael Kors Holdings','Regency Centers','L Brands','International Flavors','Nektar Therapeutics','UDR','Huntington Ingalls Indus','Torchmark','Under Armour','The Western Union','Juniper Networks','Iron Mountain','News','Avery Dennison','News','Everest Re Group','Alliant Energy','Jacobs Engineering Group','Snap-on','The Interpublic Group','Tractor Supply','SL Green Realty','Federal Realty Investment','PulteGroup','Unum','AES','Robert Half International','PerkinElmer','Pinnacle West Capital','Arconic','Affiliated Managers Group','Nordstrom','Fortune Brands Home','NiSource','Cimarex Energy','TripAdvisor','Macerich','Pentair','Allegion','Alaska Air Group','Jefferies Financial Group','Harley-Davidson','FLIR Systems','Evergy','Hanesbrands','Sealed Air','Kimco Realty','Xerox','Helmerich & Payne','Fluor','Foot Locker','Peoples United','Apartment Inv & Mgmt','Mattel','Goodyear Tire & Rubber','Leggett & Platt','Newfield Exploration','Stericycle','Envision Healthcare','Brighthouse Financial','Flowserve','Quanta Services','SCANA','Assurant','Acuity Brands','H&R Block','Range Resources']
    index = stickers.index(name);
    return stickers,sName[index]

stickers,n=getSP500('AAPL')
count = 0
sNumber = 1
for s in stickers:
    print('---------------------------')
    print('Starting to import %s'%s)
    main(s)
    count+=1
    sNumber+=1
    if (count==5):
        #wait for 61 seconds
        time.sleep(61)
        count=0
    sCounter = str(sNumber)
    print('-----#####------')
    print('Stock no %s'%sCounter)
    print('Successfully imported %s'%s)
    print('-----#####------')
    print('')
print ("Done")
