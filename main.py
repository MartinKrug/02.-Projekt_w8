# main.py
from bank_system import Bank, Transaktion, Konto
import os
import sys

def eingabe(prompt):
    """Hilfsfunktion für Benutzereingaben"""
    return input(prompt).strip()

def hauptmenu():
    """Zeigt das Hauptmenü an und verarbeitet Benutzereingaben"""
    print("\n" + "=" * 40)
    print("WILLKOMMEN ZUR PYTHON BANK".center(40))
    print("=" * 40)
    print("1. Konto eröffnen")
    print("2. An bestehendes Konto anmelden")
    print("3. Bank-Statistik anzeigen")
    print("4. Programm beenden")
    print("-" * 40)
    
    auswahl = eingabe("Ihre Auswahl: ")
    return auswahl

def kontomenu(konto, bank):
    """Zeigt das Kontomenü an und verarbeitet Benutzereingaben"""
    while True:
        print("\n" + "=" * 40)
        print(f"KONTO: {konto.kontonummer} - {konto.inhaber_name}".center(40))
        print(f"Kontostand: {konto.kontostand:.2f}€".center(40))
        print("=" * 40)
        print("1. Geld einzahlen")
        print("2. Geld abheben")
        print("3. Konto schließen")
        print("4. Zurück zum Hauptmenü")
        print("-" * 40)
        
        auswahl = eingabe("Ihre Auswahl: ")
        
        if auswahl == "1":
            try:
                betrag = float(eingabe("Einzahlungsbetrag (€): "))
                transaktion = Transaktion(konto, betrag, "Einzahlung")
                erfolg, nachricht = transaktion.durchfuehren()
                print(nachricht)
                if erfolg:
                    bank.daten_speichern()
            except ValueError:
                print("Bitte geben Sie einen gültigen Betrag ein.")
        
        elif auswahl == "2":
            try:
                betrag = float(eingabe("Abhebungsbetrag (€): "))
                transaktion = Transaktion(konto, betrag, "Abhebung")
                erfolg, nachricht = transaktion.durchfuehren()
                print(nachricht)
                if erfolg:
                    bank.daten_speichern()
            except ValueError:
                print("Bitte geben Sie einen gültigen Betrag ein.")
        
        elif auswahl == "3":
            passwort = eingabe("Bitte geben Sie Ihr Passwort ein, um das Konto zu schließen: ")
            erfolg, nachricht = konto.konto_schliessen(passwort)
            print(nachricht)
            if erfolg:
                bank.daten_speichern()
                return  # Zurück zum Hauptmenü
        
        elif auswahl == "4":
            return  # Zurück zum Hauptmenü
        
        else:
            print("Ungültige Auswahl. Bitte versuchen Sie es erneut.")


def konto_eroeffnen(bank):
    """Funktion zur Kontoeröffnung"""
    print("\n" + "=" * 40)
    print("KONTO ERÖFFNEN".center(40))
    print("=" * 40)
    
    name = eingabe("Name des Kontoinhabers: ")
    
    while True:
        passwort = eingabe("Passwort (mind. 7 Zeichen, Groß-, Kleinbuchstaben und Zahlen): ")
        if Konto.ist_passwort_gueltig(passwort):
            break
        print("Passwort erfüllt nicht die Anforderungen. Bitte erneut versuchen.")
    
    try:
        anfangsbetrag = float(eingabe("Anfangsbetrag (€): "))
        if anfangsbetrag < 0:
            print("Anfangsbetrag kann nicht negativ sein.")
            anfangsbetrag = 0
    except ValueError:
        print("Ungültiger Betrag. Anfangsbetrag wird auf 0€ gesetzt.")
        anfangsbetrag = 0
    
    konto, nachricht = bank.konto_eroeffnen(name, passwort, anfangsbetrag)
    print(nachricht)
    
    if konto:
        bank.daten_speichern()
        return konto
    
    return None

def anmelden(bank):
    """Funktion zur Kontoanmeldung"""
    print("\n" + "=" * 40)
    print("ANMELDUNG".center(40))
    print("=" * 40)
    print("1. Anmeldung mit Kontonummer")
    print("2. Suche nach Name")
    print("3. Zurück zum Hauptmenü")
    
    auswahl = eingabe("Ihre Auswahl: ")
    
    if auswahl == "1":
        kontonummer = eingabe("Kontonummer: ")
        konto = bank.konto_finden_nach_nummer(kontonummer)
        
        if konto and konto.aktiv:
            passwort = eingabe("Passwort: ")
            if konto.passwort_pruefen(passwort):
                print("Anmeldung erfolgreich!")
                kontomenu(konto, bank)
            else:
                print("Falsches Passwort.")
        else:
            print("Konto nicht gefunden oder nicht aktiv.")
    
    elif auswahl == "2":
        name = eingabe("Name des Kontoinhabers: ")
        gefundene_konten = bank.konto_finden_nach_name(name)
        
        if gefundene_konten:
            if len(gefundene_konten) == 1:
                konto = gefundene_konten[0]
                passwort = eingabe("Passwort: ")
                if konto.passwort_pruefen(passwort):
                    print("Anmeldung erfolgreich!")
                    kontomenu(konto, bank)
                else:
                    print("Falsches Passwort.")
            else:
                print(f"Es wurden {len(gefundene_konten)} Konten gefunden:")
                for i, konto in enumerate(gefundene_konten, 1):
                    print(f"{i}. Kontonummer: {konto.kontonummer}")
                
                try:
                    auswahl = int(eingabe("Wählen Sie ein Konto (Nummer): "))
                    if 1 <= auswahl <= len(gefundene_konten):
                        konto = gefundene_konten[auswahl-1]
                        passwort = eingabe("Passwort: ")
                        if konto.passwort_pruefen(passwort):
                            print("Anmeldung erfolgreich!")
                            kontomenu(konto, bank)
                        else:
                            print("Falsches Passwort.")
                    else:
                        print("Ungültige Auswahl.")
                except ValueError:
                    print("Ungültige Eingabe.")
        else:
            print("Keine Konten für diesen Namen gefunden.")



def statistik_anzeigen(bank):
    """Zeigt Bankstatistiken an"""
    stats = bank.statistik()
    
    print("\n" + "=" * 40)
    print("BANK-STATISTIK".center(40))
    print("=" * 40)
    print(f"Anzahl aktiver Konten: {stats['anzahl_konten']}")
    print(f"Gesamtsumme aller Konten: {stats['gesamtsumme']:.2f}€")
    print(f"Durchschnittlicher Kontostand: {stats['durchschnitt']:.2f}€")
    print(f"Höchster Kontostand: {stats['hoechster_kontostand']:.2f}€")
    print(f"Niedrigster Kontostand: {stats['niedrigster_kontostand']:.2f}€")
    
    eingabe("\nDrücken Sie Enter, um fortzufahren...")

def main():
    # Pfad zur Datendatei
    script_path = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(script_path, "bank_system", "bankdaten.json")
    
    # Bank initialisieren
    bank = Bank("Python Bank")
    nachricht = bank.daten_laden(data_path)
    print(nachricht)
    
    while True:
        auswahl = hauptmenu()
        
        if auswahl == "1":
            konto = konto_eroeffnen(bank)
            if konto:
                kontomenu(konto, bank)
        
        elif auswahl == "2":
            anmelden(bank)
        
        elif auswahl == "3":
            statistik_anzeigen(bank)
        
        elif auswahl == "4":
            print("Vielen Dank für die Nutzung der Python Bank. Auf Wiedersehen!")
            bank.daten_speichern(data_path)
            sys.exit(0)
        
        else:
            print("Ungültige Auswahl. Bitte versuchen Sie es erneut.")

if __name__ == "__main__":
    main()

