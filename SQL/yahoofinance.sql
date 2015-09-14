PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
/****************************************************************************\
|                                Stock Tables                                |
\****************************************************************************/

/*
Stock Information, KEY Table - StockID
*/
CREATE TABLE Stock (
StockID   integer  primary key NOT NULL,
Ticker    char(10) DEFAULT 'NA',
Active    integer  DEFAULT 1,
Name      text     DEFAULT 'NA',
FYEnds    text     DEFAULT '12-31',
Beta      real     DEFAULT '-1.0',
HasOption integer  DEFAULT 0,
Close     real     DEFAULT 0.0,
AvgVol    integer  DEFAULT 0,
Shares    integer  DEFAULT 0,
Floating  integer  DEFAULT 0,
MarketCap integer  DEFAULT 0,
Start     text     DEFAULT '0000-00-00',
End       text     DEFAULT '0000-00-00'
);
CREATE INDEX StockID ON Stock(StockID);
INSERT INTO "Stock" VALUES(1,'^GSPC',10,'^GSPC','12-31',1.0,1,0.0,0,0,0,0,'0000-00-00','0000-00-00');

/*
Stock ER dates
*/
CREATE TABLE StockER (
StockID   integer NOT NULL,
SourceID  integer NOT NULL,
RawDate   char(10),
RawTime   char(20),
FYQuarter char(6),
CYQuarter char(6),
ERDate    char(10),
FOREIGN KEY(StockID)  REFERENCES Stock(StockID),
FOREIGN KEY(SourceID) REFERENCES Source(SourceID)
);
CREATE INDEX StockERIndex ON StockER(StockID,SourceID,CYQuarter);

/*
4 Tables: Source, Sector, Industry, Source:Sector:Industry:StockID
*/
CREATE TABLE Source (
SourceID    integer primary key NOT NULL,
Name        text,
Description text);
INSERT INTO "Source" VALUES(1,'YF','Yahoo Finance');

CREATE TABLE Sector(
SectorID    integer primary key NOT NULL,
Name        text,
Description text
);

CREATE TABLE Industry(
IndustryID  integer primary key NOT NULL,
Name        text,
Description text
);

CREATE TABLE StockSector (
StockID    integer NOT NULL,
SourceID   integer NOT NULL,
SectorID   integer NOT NULL,
IndustryID integer NOT NULL,
UNIQUE(StockID, SourceID, SectorID, IndustryID) ON CONFLICT REPLACE, 
FOREIGN KEY(StockID)    REFERENCES Stock(StockID),
FOREIGN KEY(SourceID)   REFERENCES Source(SourceID),
FOREIGN KEY(SectorID)   REFERENCES Sector(SectorID),
FOREIGN KEY(IndustryID) REFERENCES Industry(IndustryID)
);

/*
CREATE TABLE DailyQuota (
.....
PertSinceCYQtr  real,  price% change since last Calendar Year Qtr End
PertSinceFYQtr  real,  price% change since last Fiscal Year Qtr End
.....
*/
CREATE TABLE DailyQuota (
StockID          integer NOT NULL,
Date             char(10),
Open             real    DEFAULT 0.0,   
High             real    DEFAULT 0.0,
Low              real    DEFAULT 0.0,
Close            real    DEFAULT 0.0,
AdjClose         real    DEFAULT 0.0,
Volume           integer DEFAULT 0,
VolumeAverage3M  integer DEFAULT 0,
VolumePerAverage real    DEFAULT 0.0,
Amount           integer DEFAULT 0,
AmountAverage3M  integer DEFAULT 0,
AmountPerAverage real    DEFAULT 0.0,
Pert1Day         real    DEFAULT 0.0,
Pert3Day         real    DEFAULT 0.0,
Pert5Day         real    DEFAULT 0.0,
Pert1Month       real    DEFAULT 0.0,
Pert1Quater      real    DEFAULT 0.0,
Pert1Year        real    DEFAULT 0.0,
PertSinceCYQtr   real    DEFAULT 0.0,
PertSinceFYQtr   real    DEFAULT 0.0,
CorrelationSP1M  real    DEFAULT 1.0,
CorrelationSP3M  real    DEFAULT 1.0,
Beta3M           real    DEFAULT 1.0,
PRIMARY KEY(StockID, Date),
FOREIGN KEY(StockID)    REFERENCES Stock(StockID)
);

CREATE INDEX DailyQuotaIndex ON DailyQuota(StockID,Date);

/****************************************************************************\
|                          Insider Transaction Tables                        |
\****************************************************************************/

/*
Insider information
*/
CREATE TABLE Insider(
InsiderID integer primary key NOT NULL,
Name      text,
Form4Url  text);
/*
Insider Transaction
*/
CREATE TABLE InsiderTrans(
StockID     integer NOT NULL,
InsiderID   integer NOT NULL,
Title       text,
Date        char(10),
BuySell     char(1),
Price       real,
Shares      integer,
Amount      real,
FOREIGN KEY(StockID) REFERENCES Stock(StockID),
FOREIGN KEY(InsiderID) REFERENCES Insider(InsiderID));

/****************************************************************************\
|                                  Views                                     |
\****************************************************************************/
CREATE VIEW StockView AS
SELECT ss.StockID, s.Ticker, s.Active, s.Name, s.FYEnds, s.Beta, s.HasOption, 
       s.Close, s.AvgVol, s.Shares, s.Floating, s.MarketCap, s.Start, s.End, 
       src.Name as Source, sec.Name AS Sector, ind.Name AS Inudstry
FROM StockSector   AS ss
LEFT JOIN Stock    AS s   ON ss.StockID=s.StockID
LEFT JOIN Source   AS src ON ss.SourceID=src.SourceID
LEFT JOIN Sector   AS sec ON ss.SectorID=sec.SectorID
LEFT JOIN Industry AS ind ON ss.IndustryID=ind.IndustryID;

CREATE VIEW StockERView AS
SELECT ss.StockID, s.Ticker, s.Active, s.Name, s.FYEnds, s.Beta, s.HasOption, 
       s.Close, s.AvgVol, s.Shares, s.Floating, s.MarketCap, s.Start, s.End, 
       src.Name as Source, sec.Name AS Sector, ind.Name AS Inudstry
FROM StockSector   AS ss
LEFT JOIN Stock    AS s   ON ss.StockID=s.StockID
LEFT JOIN Source   AS src ON ss.SourceID=src.SourceID
LEFT JOIN Sector   AS sec ON ss.SectorID=sec.SectorID
LEFT JOIN Industry AS ind ON ss.IndustryID=ind.IndustryID;

commit;
