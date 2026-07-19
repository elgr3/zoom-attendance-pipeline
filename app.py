from flask import Flask, request
import main  # importe le fichier main.py généré par Claude

app = Flask(__name__)

# On crée la route /webhook qui va recevoir les requêtes de Zoom
@app.route('/webhook', methods=['POST'])
def zoom_webhook():
    # On transmet la requête Flask directement à ta fonction GCF
    return main.main(request)

if __name__ == '__main__':
    # Le port 5000 est un standard, Render s'y connectera automatiquement
    app.run(host='0.0.0.0', port=5000)