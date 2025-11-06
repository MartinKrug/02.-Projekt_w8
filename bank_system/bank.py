# bank_system/bank.py

import json
import os
from .konto import Konto

class Bank:
    def __init__(self, name="Python Bank"):
        self.name = name
        self.konten = {}  # Dictionary mit Kontonummer als Schlüssel
        self.naechste_kontonummer = 1000  # Startpunkt für Kontonummern
        self.daten_laden()  # Bankdaten beim Start laden
    
    def konto_eroeffnen(self, inhaber_name, passwort, anfangsbetrag=0.0):
        """Eröffnet ein neues Konto"""
        # Passwort-Validierung
        if not Konto.ist_passwort_gueltig(passwort):
            return None, "Passwort erfüllt nicht die Anforderungen. Es muss mindestens 7 Zeichen lang sein und Groß-, Kleinbuchstaben und Zahlen enthalten."
        
        # Kontonummer generieren
        kontonummer = str(self.naechste_kontonummer)
        self.naechste_kontonummer += 1
        
        # Konto erstellen
        neues_konto = Konto(kontonummer, inhaber_name, passwort, float(anfangsbetrag))
        self.konten[kontonummer] = neues_konto
        
        return neues_konto, f"Konto erfolgreich eröffnet. Ihre Kontonummer ist: {kontonummer}"
    
    def konto_finden_nach_nummer(self, kontonummer):
        """Findet ein Konto anhand der Kontonummer"""
        return self.konten.get(kontonummer)
    
    def konto_finden_nach_name(self, name):
        """Findet alle Konten eines Inhabers"""
        gefundene_konten = []
        for konto in self.konten.values():
            if konto.inhaber_name.lower() == name.lower() and konto.aktiv:
                gefundene_konten.append(konto)
        return gefundene_konten
    
    def daten_laden(self, datei_pfad="bankdaten.json"):
        """Lädt Bankdaten aus einer JSON-Datei"""
        try:
            if os.path.exists(datei_pfad):
                with open(datei_pfad, 'r', encoding='utf-8') as datei:
                    daten = json.load(datei)
                    
                    # Konten laden
                    for konto_daten in daten.get("konten", []):
                        konto = Konto(
                            konto_daten["kontonummer"],
                            konto_daten["inhaber_name"],
                            konto_daten["passwort"],
                            konto_daten["kontostand"]
                        )
                        konto.aktiv = konto_daten["aktiv"]
                        self.konten[konto.kontonummer] = konto
                    
                    # Nächste Kontonummer aktualisieren
                    if daten.get("naechste_kontonummer"):
                        self.naechste_kontonummer = daten["naechste_kontonummer"]
                    else:
                        # Fallback: Höchste Kontonummer + 1
                        max_nummer = max([int(k) for k in self.konten.keys()], default=999)
                        self.naechste_kontonummer = max_nummer + 1
                    
                return True, "Bankdaten erfolgreich geladen."
            else:
                return False, "Keine Datendatei gefunden. Eine neue Bank wird erstellt."
        except Exception as e:
            return False, f"Fehler beim Laden der Bankdaten: {str(e)}"
    
    def daten_speichern(self, datei_pfad="bankdaten.json"):
        """Speichert Bankdaten in einer JSON-Datei"""
        daten = {
            "name": self.name,
            "naechste_kontonummer": self.naechste_kontonummer,
            "konten": [konto.to_dict() for konto in self.konten.values()]
        }
        
        try:
            with open(datei_pfad, 'w', encoding='utf-8') as datei:
                json.dump(daten, datei, indent=4, ensure_ascii=False)
            return True, "Bankdaten erfolgreich gespeichert."
        except Exception as e:
            return False, f"Fehler beim Speichern der Bankdaten: {str(e)}"
    
    def statistik(self):
        """Erstellt eine einfache Statistik über alle Konten"""
        aktive_konten = [k for k in self.konten.values() if k.aktiv]
        gesamtsumme = sum(k.kontostand for k in aktive_konten)
        durchschnitt = gesamtsumme / len(aktive_konten) if aktive_konten else 0
        
        return {
            "anzahl_konten": len(aktive_konten),
            "gesamtsumme": gesamtsumme,
            "durchschnitt": durchschnitt,
            "hoechster_kontostand": max([k.kontostand for k in aktive_konten], default=0),
            "niedrigster_kontostand": min([k.kontostand for k in aktive_konten], default=0)
        }
