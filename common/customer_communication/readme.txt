f"""
Aanmaakdatum: {date.today().strftime("%d-%m-%Y")}
Versie: {self.version}
---------------------------------------------------------
Bestandslevering van {len(self.data)} records voor "{self.client_name}"
---------------------------------------------------------


MATRIXIAN GROUP
---------------
www.matrixiangroup.com
info@matrixiangroup.com
+31 (0)20 244 0145
Klantnaam: {self.client_name}


BESTANDEN
--------
{self.file_name}
{self.readme}.txt
{self.codebook}.xlsx


OMSCHRIJVING
---------
{self.objective}
Product: {self.product_doc}

DATA OVERZICHT
-------------
Aantal variabelen:\t {self.num_var}
Aantal records:\t\t {self.num_records}
Lege waardes:\t\t {self.empty_cells} ({self.perc_empty_cells} %)
Numerieke waardes:\t {self.numeric_fields}
Tekstuele waardes:\t {self.text_fields}
Datumwaardes :\t\t {self.date_fields}
Boolean waardes:\t {self.bool_fields}


DATAVELDEN
-----------
{self.cols}


EENHEDEN
--------------------
Ruimtelijk: in metrische meters
Datums: YYYY-MM-DD
Boolean: 1 = True, 0 = False
Valuta: in euro's (EUR/€)


GETTING STARTED
--------------------
Naast de data, ontvangt u van ons ook een readme bestand en een codeboek. Deze twee automatisch gegenereerde bestanden dienen als ondersteuning van het databestand.
In het readme-bestand staat een beknopte omschrijving van de dataset, de meegeleverde bestanden en onze contact informatie.
Voor een uitgebreide verklaring van de dataset leveren wij ook een codeboek mee. 
In het codeboek kunt u het data profiel en de verbose omschrijving van elke kolom inzien. 

Voor verdere vragen en onduidelijkheden, kunt u natuurlijk altijd met ons contact opnemen.

Matrixian Group


"""