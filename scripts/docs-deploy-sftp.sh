#!/bin/sh
# Synchronisiert die gebaute Doku per SFTP auf den Webserver.
#
# Ersetzt `docs-deploy` aus dem Builder-Image, das ausschliesslich FTP kann
# (unverschluesselt, und der ProFTPD am Ziel meldet in PASV eine falsche
# Adresse). Bis das zentrale Image SFTP beherrscht, liegt der Weg hier.
# Verhalten und Schutzmechanismen sind bewusst identisch zum FTP-Original --
# nur der Transport ist ein anderer.
#
# Pflicht-Umgebungsvariablen:
#   DOC_SFTP_HOST      Servername oder IP
#   DOC_SFTP_USER      Benutzer
#   DOC_SFTP_PASSWORD  Passwort (als 'maskiert' anlegen)
#   DOC_SFTP_PATH      Zielverzeichnis am Server. Wird bewusst NICHT aus
#                      DOCS_URL abgeleitet -- der Auslieferungsort soll
#                      ausdruecklich dastehen.
#
# Optional:
#   DOCS_OUTPUT        Quellverzeichnis (Default: public)
#   DOC_SFTP_PORT      Port (Default: 22)
#   DOC_SFTP_KNOWN_HOSTS  Erwarteter Hostschluessel im known_hosts-Format.
#                      Ohne ihn wird der Schluessel blind angenommen: der
#                      Transport ist dann gegen Mitlesen geschuetzt, nicht
#                      gegen einen aktiven Zwischenmann.
#   DOC_SFTP_DRY_RUN   auf "1" setzen: zeigt nur, was passieren wuerde

set -eu

ZIEL="${DOCS_OUTPUT:-public}"
PORT="${DOC_SFTP_PORT:-22}"

fehlt=""
for name in DOC_SFTP_HOST DOC_SFTP_USER DOC_SFTP_PASSWORD DOC_SFTP_PATH; do
	eval "wert=\${$name:-}"
	[ -n "$wert" ] || fehlt="$fehlt $name"
done
if [ -n "$fehlt" ]; then
	echo "FEHLER: Fehlende Umgebungsvariablen:$fehlt" >&2
	echo >&2
	echo "Anzulegen unter Settings > CI/CD > Variables." >&2
	echo "DOC_SFTP_PASSWORD als 'maskiert'." >&2
	if [ "${#fehlt}" -gt 45 ] && [ -n "${CI_COMMIT_BRANCH:-}" ]; then
		echo >&2
		echo "Es fehlen ALLE Variablen. Haeufigste Ursache: sie sind als" >&2
		echo "'protected' angelegt, der Branch '$CI_COMMIT_BRANCH' ist es aber" >&2
		echo "nicht. Geschuetzte Variablen sind dann fuer den Job unsichtbar," >&2
		echo "obwohl sie in der Oberflaeche dastehen." >&2
		echo "Pruefen unter Settings > Repository > Protected branches." >&2
	fi
	exit 1
fi

if [ ! -d "$ZIEL" ]; then
	echo "FEHLER: '$ZIEL/' nicht gefunden. Laeuft docs-build vorher?" >&2
	exit 1
fi

for werkzeug in lftp ssh sshpass; do
	command -v "$werkzeug" >/dev/null 2>&1 || {
		echo "FEHLER: '$werkzeug' fehlt. Im Job installieren:" >&2
		echo "  apk add --no-cache openssh-client sshpass" >&2
		exit 1
	}
done

# Hostschluessel: gepinnt, wenn DOC_SFTP_KNOWN_HOSTS gesetzt ist, sonst blind
# angenommen. Blind heisst: gegen Mitlesen geschuetzt, nicht gegen einen
# aktiven Zwischenmann -- fuer eine oeffentliche Doku vertretbar, den
# Schluessel zu hinterlegen ist aber besser.
KNOWN_HOSTS="$(mktemp)"
trap 'rm -f "$KNOWN_HOSTS"' EXIT INT TERM
if [ -n "${DOC_SFTP_KNOWN_HOSTS:-}" ]; then
	printf '%s\n' "$DOC_SFTP_KNOWN_HOSTS" >"$KNOWN_HOSTS"
	SSH_PRUEFUNG="-o StrictHostKeyChecking=yes -o UserKnownHostsFile=$KNOWN_HOSTS"
else
	echo "HINWEIS: DOC_SFTP_KNOWN_HOSTS ist nicht gesetzt -- der Hostschluessel"
	echo "         wird ungeprueft angenommen. Zum Pinnen am Server ausfuehren:"
	echo "         ssh-keyscan -p $PORT <host>"
	SSH_PRUEFUNG="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"
fi

# sshpass -e liest das Passwort aus SSHPASS. Bewusst nicht -p: sonst stuende
# es in der Prozessliste des Runners und in jeder lftp-Debugausgabe.
SSHPASS="$DOC_SFTP_PASSWORD"
export SSHPASS

verbindung() {
	printf 'set sftp:connect-program "sshpass -e ssh -a -x -p %s %s"; ' \
		"$PORT" "$SSH_PRUEFUNG"
	printf 'set sftp:auto-confirm yes; '
	printf 'set net:timeout 20; '
	printf 'set net:max-retries 2; '
	printf 'set cmd:fail-exit true; '
	printf 'open -u "%s","%s" "sftp://%s"; ' \
		"$DOC_SFTP_USER" "$DOC_SFTP_PASSWORD" "$DOC_SFTP_HOST"
}

TROCKEN=""
if [ "${DOC_SFTP_DRY_RUN:-}" = "1" ]; then
	TROCKEN="--dry-run"
	echo "TROCKENLAUF -- es wird nichts uebertragen oder geloescht."
fi

# Schutz vor einem nicht eingesperrten Konto.
#
# Ist der Account nicht auf sein Verzeichnis beschraenkt, zeigt DOC_SFTP_PATH="/"
# auf das Dateisystem-Wurzelverzeichnis des Servers -- und "--delete" beginnt,
# /bin, /etc und /lib abzuraeumen. Genau das ist beim FTP-Weg schon passiert.
VERDAECHTIG="$(lftp -c "$(verbindung) cd \"$DOC_SFTP_PATH\"; cls -1" 2>/dev/null |
	tr -d '/' |
	grep -cxE 'bin|sbin|etc|proc|sys|dev|lib|usr|var|root|boot' || true)"
if [ "$VERDAECHTIG" -ge 3 ]; then
	echo "FEHLER: '$DOC_SFTP_PATH' sieht aus wie ein Systemverzeichnis, nicht wie" >&2
	echo "ein Webspace ($VERDAECHTIG typische Systemordner gefunden)." >&2
	echo "Vermutlich ist der SFTP-Account nicht auf sein Verzeichnis beschraenkt." >&2
	echo "Abbruch, bevor --delete dort etwas entfernt." >&2
	exit 1
fi

# Vorabpruefung: darf das Konto im Zielverzeichnis ueberhaupt schreiben und
# loeschen? Ohne sie erschiene ein Rechteproblem erst als hunderte Zeilen
# "Access failed: Permission denied" mitten im Abgleich.
PROBE=".ormpp-schreibprobe"
: >"$PROBE"
if ! lftp -c "$(verbindung) put -O \"$DOC_SFTP_PATH\" \"$PROBE\"; rm \"$DOC_SFTP_PATH/$PROBE\"" >/dev/null 2>&1; then
	rm -f "$PROBE"
	echo "FEHLER: '$DOC_SFTP_USER' darf in '$DOC_SFTP_PATH' nicht schreiben oder loeschen." >&2
	echo >&2
	echo "Haeufigste Ursache nach dem Wechsel von FTP auf SFTP: der Bestand" >&2
	echo "gehoert noch dem alten FTP-Konto, das SFTP-Konto ist ein anderer" >&2
	echo "Benutzer. Am Server einmalig uebereignen, etwa:" >&2
	echo "  chown -R $DOC_SFTP_USER '$DOC_SFTP_PATH'" >&2
	exit 1
fi
rm -f "$PROBE"

# Jeder mirror-Aufruf muss auf EINER Zeile stehen: lftp trennt Befehle am
# Zeilenumbruch, ein umbrochenes mirror wird als "mirror" ohne Argumente
# gelesen und spiegelt dann das gesamte Arbeitsverzeichnis inklusive .git.
#
# --ignore-time  vergleicht nur die Groesse. Noetig, weil der Git-Checkout in
#                der Pipeline allen Dateien die aktuelle Zeit gibt; sonst gilt
#                jede Datei als neuer und alles wuerde uebertragen.
# --overwrite    schreibt direkt ueber die bestehende Datei. Ohne das loescht
#                lftp erst und laedt dann hoch -- in diesem Fenster liefert der
#                Server 404.
# --delete       entfernt am Ziel, was lokal fehlt. Geschieht NACH den Uploads,
#                es gibt also keinen Moment mit kaputten Verweisen.
# --no-perms     ueberspringt das chmod nach jeder Uebertragung. Die Modi aus
#                dem CI-Checkout sagen ueber den Webspace nichts aus, und ein
#                Konto ohne chmod-Recht scheitert daran ohne Not.
# spiegeln fuehrt einen mirror aus und laesst die Pipeline scheitern, wenn
# auch nur eine Datei nicht durchkam.
#
# Bewusst NICHT `lftp ... | tee`: der Exit-Status einer Pipe ist der des
# letzten Glieds, lftps Fehlschlag ginge verloren und der Job waere gruen,
# obwohl die Doku unveraendert am Server liegt. Und lftp meldet Fehler
# einzelner Dateien im mirror ohnehin nur im Text, nicht im Exit-Status --
# deshalb zusaetzlich die Ausgabe pruefen.
spiegeln() {
	AUSGABE="$(mktemp)"
	RC=0
	lftp -c "$(verbindung) $1" >"$AUSGABE" 2>&1 || RC=$?
	cat "$AUSGABE"

	FEHLZEILEN="$(grep -c "Access failed\|Fatal error" "$AUSGABE" || true)"
	if [ "$RC" -eq 0 ] && [ "$FEHLZEILEN" -eq 0 ]; then
		rm -f "$AUSGABE"
		return 0
	fi
	echo >&2
	echo "FEHLER: Der Abgleich ist nicht vollstaendig durchgelaufen" >&2
	echo "($FEHLZEILEN fehlgeschlagene Datei-Operationen, lftp-Status $RC)." >&2
	if grep -q "Permission denied" "$AUSGABE"; then
		echo >&2
		echo "Das Zielverzeichnis ist beschreibbar (die Vorabpruefung lief durch)," >&2
		echo "einzelne Dateien darin aber nicht — sie gehoeren einem anderen" >&2
		echo "Benutzer, typischerweise dem alten FTP-Konto. Am Server einmalig" >&2
		echo "uebereignen:" >&2
		echo "  chown -R $DOC_SFTP_USER '$DOC_SFTP_PATH'" >&2
	fi
	rm -f "$AUSGABE"
	exit 1
}

echo "Sync: $ZIEL/ -> sftp://$DOC_SFTP_HOST$DOC_SFTP_PATH"
spiegeln "mirror --reverse --delete --ignore-time --overwrite --no-perms --parallel=4 --verbose $TROCKEN \"$ZIEL\" \"$DOC_SFTP_PATH\""

# Dateien mit stabilem Namen immer uebertragen. --ignore-time vergleicht nur die
# Groesse; eine Aenderung, die die Byte-Groesse nicht veraendert (etwa
# "recieve" -> "receive"), wuerde sonst uebersehen und nie ausgeliefert.
# Assets in _astro/ und pagefind/ sind davon nicht betroffen, weil Astro sie
# nach Inhalts-Hash benennt: anderer Inhalt bedeutet dort anderer Dateiname.
echo "Erzwinge Dateien mit stabilem Namen ..."
spiegeln "mirror --reverse --transfer-all --overwrite --no-perms --parallel=4 --verbose $TROCKEN --include-glob=*.html --include-glob=*.xml --include-glob=*.json --include-glob=*.txt \"$ZIEL\" \"$DOC_SFTP_PATH\""

echo "Sync abgeschlossen."
