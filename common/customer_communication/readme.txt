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
Contactpersoon: {self.contact_person}
Klantnaam: {self.client_name}


INHOUD
--------
{self.file_name}
{self.readme}.txt
{self.codebook}.xlsx


KLANTVRAAG
---------
Beschrijving: {self.objective}
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


DISCLAIMER
--------------------
Interpretatie van de waardes
"""