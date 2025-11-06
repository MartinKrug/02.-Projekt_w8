# bank_system/konto.py
import re

class Konto:
    def __init__(self, kontonummer, inhaber_name, passwort, kontostand=0.0):
        self.kontonummer = kontonummer
        self.inhaber_name = inhaber_name
        self.kontostand = kontostand
        self._passwort = passwort  
        self.aktiv = True
        self.transaktionen = []
    
    def to_dict(self):
        """Konvertiert Kontodaten in ein Dictionary für JSON-Speicherung"""
        return {
            "kontonummer": self.kontonummer,
            "inhaber_name": self.inhaber_name,
            "kontostand": self.kontostand,
            "passwort": self._passwort,
            "aktiv": self.aktiv
        }
    
    @staticmethod
    def ist_passwort_gueltig(passwort):
        """Überprüft, ob das Passwort den Anforderungen entspricht"""
        if len(passwort) < 7:
            return False
        if not re.search(r'[A-Z]', passwort):  # Mindestens ein Großbuchstabe
            return False
        if not re.search(r'[a-z]', passwort):  # Mindestens ein Kleinbuchstabe
            return False
        if not re.search(r'[0-9]', passwort):  # Mindestens eine Zahl
            return False
        return True
    
    def passwort_pruefen(self, passwort):
        """Überprüft, ob das eingegebene Passwort korrekt ist"""
        return self._passwort == passwort
    
    def einzahlen(self, betrag):
        """Einzahlung auf das Konto"""
        if betrag <= 0:
            return False, "Der Betrag muss positiv sein."
        
        self.kontostand += betrag
        return True, f"{betrag}€ wurden eingezahlt. Neuer Kontostand: {self.kontostand}€"
    
    def abheben(self, betrag):
        """Geld vom Konto abheben"""
        if betrag <= 0:
            return False, "Der Betrag muss positiv sein."
        
        if betrag > self.kontostand:
            return False, "Nicht genügend Guthaben vorhanden."
        
        self.kontostand -= betrag
        return True, f"{betrag}€ wurden abgehoben. Neuer Kontostand: {self.kontostand}€"
    
    def konto_schliessen(self, passwort):
        """Schließt das Konto, wenn das Passwort korrekt ist"""
        if not self.passwort_pruefen(passwort):
            return False, "Passwort ungültig."
        
        if self.kontostand > 0:
            return False, f"Bitte heben Sie zuerst Ihr Guthaben ab: {self.kontostand}€"
        
        self.aktiv = False
        return True, "Konto wurde erfolgreich geschlossen."


