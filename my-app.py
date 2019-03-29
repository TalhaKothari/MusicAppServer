from flask import Flask, request, jsonify, json
from cassandra.cluster import Cluster
import requests
import sys
import uuid

cluster = Cluster(['cassandra'])
#cluster = Cluster(['127.0.0.1'])
session = cluster.connect()

app = Flask(__name__)

@app.route('/')
def hello():
  name = request.args.get("name","World")
  return('<h1>Hello, {}!</h1>'.format(name))


@app.route('/music', methods=['GET'])
def get_all_music():
  music_data=[]
  rows = session.execute("SELECT * FROM music.artist")
  for row in rows:
    print(row.id,row.name,row.album, file=sys.stderr)
    music = {'id': row.id, 'name': row.name, 'album': row.album}
    music_data.append(music)
  return jsonify(music_data), 200


@app.route('/music/<string:id>', methods=['GET'])
def get_artist_by_id(id):
  print('inside get_artist_by_id')
  print(uuid.UUID(id))
  music_data=[]
  rows = session.execute("""SELECT * FROM music.artist WHERE id=%(id)s""",{'id': uuid.UUID(id)})
  for row in rows:
    music = {'id': row.id, 'name': row.name, 'album': row.album}
    music_data.append(music)
  return jsonify(music_data), 200


@app.route('/music', methods=['POST'])
def create_artist():
  print('inside create_artist')
  rows = session.execute("INSERT INTO music.artist (id, name, album) VALUES (%s, %s, %s)", (uuid.uuid1(), request.form['name'], request.form['album']))
  return jsonify({'message':'new record created'}), 201


@app.route('/music/<string:id>', methods = ['PUT'])
def update_artist(id):
  print('inside update_artist')
  rows = session.execute("""UPDATE music.artist SET name=%(name)s, album=%(album)s WHERE id=%(id)s""", {'name': request.form['name'], 'album': request.form['album'], 'id': uuid.UUID(id)})
  print(rows,file=sys.stderr)

  return jsonify({'message':'updated successfully'}), 200


@app.route('/music/<string:id>', methods = ['DELETE'])
def delete_artist(id):
  print('inside delete_artist')
  session.execute("""DELETE FROM music.artist WHERE id=%(id)s""", { 'id': uuid.UUID(id)})
  return jsonify({'message':'delete successfull'}), 200


if __name__ == '__main__':
        app.run(host='0.0.0.0', port=8080)