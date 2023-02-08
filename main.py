import json
import sqlalchemy
import sqlalchemy as sq
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
from dk_command import command_list, dk_help

DIALECT = 'postgresql'
USERNAME = 'postgres'
PASSWORD = 'dimkanet'
HOST = 'localhost'
PORT = 5432
DATABASE = 'bookshop'

# dialect+driver://username:password@host:port/database
DSN = f"{DIALECT}://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"

# Задание 1. Составить модели классов SQLAlchemy по схеме:
Base = declarative_base()


class Publisher(Base):
    __tablename__ = "publisher"
    id = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String(length=40), unique=True)


class Book(Base):
    __tablename__ = "book"
    id = sq.Column(sq.Integer, primary_key=True)
    title = sq.Column(sq.String(length=120))
    id_publisher = sq.Column(sq.Integer, sq.ForeignKey("publisher.id"), nullable=True)
    publisher = relationship(Publisher, backref="publishers")


class Shop(Base):
    __tablename__ = "shop"
    id = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String(length=40), unique=True)


class Stock(Base):
    __tablename__ = "stock"
    id = sq.Column(sq.Integer, primary_key=True)
    id_book = sq.Column(sq.Integer, sq.ForeignKey("book.id"), nullable=True)
    id_shop = sq.Column(sq.Integer, sq.ForeignKey("shop.id"), nullable=True)
    count = sq.Column(sq.Integer, nullable=False)
    book = relationship(Book, backref="books")
    shop = relationship(Shop, backref="shops")


class Sale(Base):
    __tablename__ = "sale"
    id = sq.Column(sq.Integer, primary_key=True)
    price = sq.Column(sq.Float)
    date_sale = sq.Column(sq.DateTime)
    id_stock = sq.Column(sq.Integer, sq.ForeignKey("stock.id"), nullable=True)
    count = sq.Column(sq.Integer, nullable=False)
    stock = relationship(Stock, backref="stocks")


# Задание 2
#
# Используя SQLAlchemy, составить запрос выборки магазинов, продающих целевого издателя.
#
# Напишите Python скрипт, который:
#
# Подключается к БД любого типа на ваш выбор (например, к PostgreSQL). Импортирует необходимые модели данных.
# Принимает имя или идентификатор издателя (publisher), например через input(). Выводит построчно факты покупки книг
# этого издателя:


def create_tables(engine_t):    # Создаем таблицы в БД
    Base.metadata.create_all(engine_t)
    print("Таблицы созданы")


def drop_tables(engine_t):      # Удаляем таблицы в БД
    Base.metadata.drop_all(engine_t)
    print("Таблицы удалены")


# Задание 3 (необязательное)
# Заполните БД тестовыми данными.

def insert_data(session_i):     # Заполняем таблицы в БД
    with open('fixtures/tests_data.json', 'r') as fd:
        data = json.load(fd)

    for record in data:
        model = {
            'publisher': Publisher,
            'shop': Shop,
            'book': Book,
            'stock': Stock,
            'sale': Sale,
        }[record.get('model')]
        session_i.add(model(id=record.get('pk'), **record.get('fields')))
    session_i.commit()
    print("Таблицы заполнены")


def view_publisher(session_v):      # Выводим список издателей с их ID
    q = session_v.query(Publisher)
    print('[id] Издатель')
    for s in q.all():
        print(f'[{s.id}] {s.name}')


def view_sales(session_v):          # Ищем продажи по ID издателя
    id_pub = input("Введите id Издателя: ")

    q2 = session_v.query(Shop.name, Book.title, Sale.price, Sale.date_sale). \
        filter(Sale.id_stock == Stock.id).filter(Shop.id == Stock.id_shop). \
        filter(Book.id == Stock.id_book).filter(Publisher.id == Book.id_publisher). \
        filter(Publisher.id == id_pub)
    for s in q2.all():
        print(f'{s.name: <20} | {s.title:<40} | {s.price:<10} | {s.date_sale.strftime("%d-%m-%Y")}')


if __name__ == '__main__':

    engine = sqlalchemy.create_engine(DSN)
    Session = sessionmaker(bind=engine)
    session = Session()

    while True:
        command = input('[Введите команду]: ')
        if not (command in command_list):
            print('Введена неизвестная команда (для вывода списка команд введите help).')
            continue

        match command:
            case 'exit':
                print("Выход из программы. Все наилучшего!")
                break
            case 'help':
                dk_help()
                continue
            case 'doall':
                drop_tables(engine)
                session.commit()
                create_tables(engine)
                session.commit()
                insert_data(session)
                continue
            case 'createtables':
                create_tables(engine)
                session.commit()
                continue
            case 'droptables':
                drop_tables(engine)
                session.commit()
                continue
            case 'insertdata':
                insert_data(session)
                continue
            case 'listp':
                view_publisher(session)
                continue
            case 'pub':
                view_sales(session)
                continue
