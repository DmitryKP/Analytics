from flask import Flask
from flask_restful import Resource, reqparse, Api
from datetime import datetime

app = Flask(__name__)
api = Api(app)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///base.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['PROPAGATE_EXCEPTIONS'] = True

from base import Contacts, db
db.init_app(app)
app.app_context().push()
db.create_all()

class Contacts_List(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('segment', type=str, required=False, help='Segment name')
    parser.add_argument('email', type=str, required=False, help='Email contact')
    parser.add_argument('phone', type=str, required=False, help='Phone contact')
    
    def get(self, segment):
        item = Contacts.find_by_segment(segment)
        if item:
            return item.json()
        return {'Message': 'Contacts are not found'}
    
    def post(self):
        args = Contacts_List.parser.parse_args()
        item = Contacts(str(datetime.today().strftime('%Y.%m.%d') ), args['segment'], args['email'], args['phone'])
        item.save_to()
        return item.json()
        
    def put(self):
        return 'PUT Method not available'
            
    def delete(self):
        return 'DEL Method not available'
    
class All_Contacts(Resource):
    def get(self):
        return {'All_Contacts': list(map(lambda x: x.json(), Contacts.query.all()))}
    
api.add_resource(All_Contacts, '/all')
api.add_resource(Contacts_List, '/add')

if __name__=='__main__':
    
    app.run(host="xxx", port=xxx)
