
# bank_system/transaktion.py
from datetime import datetime

class Transaktion:
    def __init__(self, konto, betrag, typ, beschreibung=""):
        self.konto = konto
        self.betrag = betrag
        self.typ = typ  # "Einzahlung", "Abhebung", etc.
        self.beschreibung = beschreibung
        self.zeitstempel = datetime.now()
        self.erfolgreich = False
        self.nachricht = ""
    
    def durchfuehren(self):
        """Führt die Transaktion durch"""
        if self.typ == "Einzahlung":
            self.erfolgreich, self.nachricht = self.konto.einzahlen(self.betrag)
        elif self.typ == "Abhebung":
            self.erfolgreich, self.nachricht = self.konto.abheben(self.betrag)
        else:
            self.erfolgreich = False
            self.nachricht = "Ungültiger Transaktionstyp"
        
        # Transaktion zur Konto-History hinzufügen, wenn erfolgreich
        if self.erfolgreich:
            self.konto.transaktionen.append(self)
        
        return self.erfolgreich, self.nachricht
    
    def to_dict(self):
        """Konvertiert Transaktionsdaten in ein Dictionary für JSON-Speicherung"""
        return {
            "kontonummer": self.konto.kontonummer,
            "betrag": self.betrag,
            "typ": self.typ,
            "beschreibung": self.beschreibung,
            "zeitstempel": self.zeitstempel.isoformat(),
            "erfolgreich": self.erfolgreich,
            "nachricht": self.nachricht
        }

