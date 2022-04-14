import psycopg2
import csv
import time


def mark(string):
    if string != 'null' and string is not None:
        return float(string.replace(',', '.'))

ZNO2019 = 'Odata2019File.csv'
ZNO2021 = 'Odata2021File.csv'

create = '''
CREATE TABLE IF NOT EXISTS tbl_ZNO (
    outID VARCHAR (36) PRIMARY KEY,
    birth INT NOT NULL,
    sexType VARCHAR (255) NOT NULL,
    regname VARCHAR (255),
    engMark REAL,
    engTestStatus BOOLEAN,
    year INT NOT NULL
);
'''

insert = '''
INSERT INTO tbl_ZNO (outID, birth, sexType, regname, engMark, engTestStatus, year)
VALUES (%s, %s, %s, %s, %s, %s, %s)
'''

query = '''
SELECT regname2019 AS regname, engmark2019, engmark2021
FROM (SELECT regname AS regname2019, MAX(engmark) AS engmark2019 FROM tbl_ZNO
	  WHERE engteststatus = true AND year = 2019
	  GROUP BY regname) AS tbl_zno2019,
	 (SELECT regname AS regname2021, MAX(engmark) AS engmark2021 FROM tbl_ZNO
	  WHERE engteststatus = true AND year = 2021
	  GROUP BY regname) AS tbl_zno2021
WHERE regname2019 = regname2021
'''

tries = 5
while tries:
    try:
        conn = psycopg2.connect(dbname='znodata', user='postgres', password='password', host='db')

        with conn:
            cur = conn.cursor()

            # cur.execute('DROP TABLE IF EXISTS tbl_ZNO')
            cur.execute(create)

            cur.execute('SELECT COUNT(outID) FROM tbl_ZNO WHERE year=2019')
            count = cur.fetchone()[0]

            start = time.time()

            with open(ZNO2019, 'r', encoding='windows-1251') as csvfile:
                reader = csv.reader(csvfile, delimiter=';')
                headers = next(reader)

                outid = headers.index('OUTID')
                birth = headers.index('Birth')
                sextypename = headers.index('SEXTYPENAME')
                regname = headers.index('REGNAME')
                engBall100 = headers.index('engBall100')
                engTestStatus = headers.index('engTestStatus')

                for _ in range(count):
                    next(reader)
                
                for row in reader:
                    values = (row[outid], row[birth], row[sextypename], row[regname], mark(row[engBall100]), row[engTestStatus] != 'null', 2019)
                    cur.execute(insert, values)

            cur.execute('SELECT COUNT(outID) FROM tbl_ZNO WHERE year=2021')
            count = cur.fetchone()[0]
	    
            with open(ZNO2021, 'r', encoding='utf-8-sig') as csvfile:
                reader = csv.reader(csvfile, delimiter=';')
                headers = next(reader)

                outid = headers.index('OUTID')
                birth = headers.index('Birth')
                sextypename = headers.index('SexTypeName')
                regname = headers.index('RegName')
                engBall100 = headers.index('EngBall100')
                engTestStatus = headers.index('EngTestStatus')

                for _ in range(count):
                    next(reader)
                
                for row in reader:
                    values = (row[outid], row[birth], row[sextypename], row[regname], mark(row[engBall100]), row[engTestStatus] != 'null', 2021)
                    cur.execute(insert, values)

        with open('execution time.txt', 'w') as timefile:
            timefile.write(f'Execution time: {time.time() - start}')
            print(time.time() - start)

        cur.execute(query)

        with open('ZNOdata.csv', 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile, delimiter=';')
            writer.writerow([col[0] for col in cur.description])

            for row in cur:
                writer.writerow([str(el) for el in row])
        
        tries = 0
    
    except psycopg2.OperationalError as err:
        tries -= 1
        print('OperationalError')
        # print(err)
        time.sleep(1.5)
    
    except FileNotFoundError as err:
        tries = 0
        print('FileNotFoundError')
        # print(f'File {err.filename} does not exist')

