# Bot Discord Python pour la musique dans les salons vocaux.

Sources prises en charge :
- YouTube
- Spotify
- Deezer
- SoundCloud
- Bandcamp


## 1. Prérequis

Applications :
- [Python 3.12](https://www.python.org/downloads/windows/) ou plus récent
- [FFmpeg](https://www.ffmpeg.org/download.html)
- [Visual Studio Code](https://code.visualstudio.com/) optionnel

Dépendances Python :
- `discord.py`
- `PyNaCl`
- `python-dotenv`
- `requests`
- `yt-dlp`
- `Pillow`
- `deezer-python`

Ces dépendances sont déjà listées dans [requirements.txt](C:/Users/Boiled/Documents/New%20project/requirements.txt).

## 2. Installation de FFmpeg

Sous Windows :

1. Télécharge FFmpeg.
2. Décompresse l'archive.
3. Renomme le dossier en `ffmpeg`.
4. Place-le dans `C:\`.

Le chemin final doit ressembler à :
```text
C:\ffmpeg\
```

Ajoute ensuite `C:\ffmpeg\bin` au `PATH` Windows :

1. Ouvre `Modifier les variables d'environnement système`
2. Va dans `Variables d'environnement`
3. Sélectionne `Path`
4. Clique sur `Modifier`
5. Ajoute :

```text
C:\ffmpeg\bin
```

## 3. Installation du projet

Dans le dossier du bot :

```bash
python -m venv venv
venv\Scripts\activate
python -m pip install -U pip
pip install -r requirements.txt
```

## 4. Création du bot Discord

Crée ton bot sur [Discord Developer Portal](https://discord.com/developers/applications).

À faire :
- créer une application
- aller dans `Bot`
- activer les intents nécessaires

Intents nécessaires pour cette version :
- `Message Content Intent`

Récupère ensuite le token du bot et garde-le privé.

Permissions conseillées pour l'invitation du bot :
- Voir les salons
- Envoyer des messages
- Intégrer des liens
- Joindre des fichiers
- Gérer les messages
- Se connecter
- Parler
- Utiliser la détection d'activité vocale
- Lire l'historique des messages

## 5. Fichier `.env`

Crée ou modifie le fichier `.env` à la racine du projet avec au minimum :

```env
TOKEN=ton_token_discord
BOT_USE_ENV_PROXY=false
```

Note importante :
- cette version de `discordbot.py` ne demande pas de clés Spotify Developer
- il n'est donc pas nécessaire de créer une application Spotify pour faire fonctionner Spotify

## 6. Lancement du bot

Commande directe :

```bash
python Renaudbot4.py
```

Si tu utilises l'environnement virtuel :

```bash
venv\Scripts\python.exe Renaudbot4.py
```

## 7. Commandes principales

Préfixe :

```text
r!
```

Commandes :
- `r!play <lien ou recherche>`
- `r!skip`
- `r!skip <nombre>`
- `r!previous`
- `r!previous <nombre>`
- `r!pause`
- `r!resume`
- `r!stop`
- `r!leave`
- `r!seek <timecode>`
- `r!queue`
- `r!clear`
- `r!shuffle`
- `r!loop`
- `r!loop_queue`
- `r!help`

Exemples :

```text
r!play https://youtu.be/xxxxxxxxxxx
r!play Akira Yamaoka Promise
r!skip 3
r!previous 2
r!seek 1:30
```

## 8. Notes

- le terminal du bot doit rester ouvert, sinon le bot s'arrête
- si `ffmpeg` n'est pas trouvé, vérifie le `PATH`
- si le vocal Discord ne fonctionne pas, vérifie aussi `PyNaCl`
- pour une nouvelle machine, le plus simple est :

```bash
pip install -r requirements.txt
```
