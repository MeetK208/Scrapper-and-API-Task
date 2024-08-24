from flask import Flask, jsonify, request
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///books.db'
db = SQLAlchemy(app)

class Book(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(200), nullable=False)
    author = db.Column(db.String(200), nullable=False)
    Publishedyear = db.Column(db.Integer, nullable=False)
    createdDatetime = db.Column(db.DateTime, default=datetime.utcnow)

@app.route('/books', methods=['GET'])
def get_books():
    try:
        # Retrieve and validate page and limit parameters
        page = request.args.get('page', 1, type=int)
        limit = request.args.get('limit', 10, type=int)

        if page < 1:
            page = 1
        if limit < 1:
            limit = 10

        # Apply pagination
        pagination = Book.query.paginate(page=page, per_page=limit, error_out=False)
        books_data = [{
            'id': book.id,
            'title': book.title,
            'author': book.author,
            'Publishedyear': book.Publishedyear,
            'createdDatetime': book.createdDatetime.isoformat()
        } for book in pagination.items]

        return jsonify({
            'page': pagination.page,
            'per_page': pagination.per_page,
            'total': pagination.total,
            'total_pages': pagination.pages,
            'data': books_data
        }), 200
    except SQLAlchemyError as e:
        return jsonify({'error': 'Database error', 'message': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
