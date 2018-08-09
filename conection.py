#!/usr/bin/python3
import psycopg2
import psycopg2.extras

# def connectToDB(connectionString):
# 	connectionString = 'dbname=utilities user=andre password=13out1903 host=localhost'
# 	print(connectionString)
# 	try:
# 		return psycopg2.connect(connectionString)C
#  except:
# 	 	print("Can't connect to database")

class ConnPostgreSQL:

	def __init__(self, dbnome, user, passwd, host="localhost"):
		connectionString = "dbname=%s user=%s password=%s host=%s"\
			% (dbnome, user, passwd, host)
		# print(connectionString)
		self.con = psycopg2.connect(connectionString)
		self.cur = self.con.cursor(cursor_factory=psycopg2.extras.DictCursor)

	def commit(self):
		self.con.commit()

	def rollback(self):
		self.con.rollback()


class PostCrud:
	signals = {'lt': '<', 'le':'<=', 'eq':'=',
	           'df':'!=', 'ge':'>=', 'gt':'>'}

	def __init__(self, connection, model, **kwargs):
		self.conn = connection
		self.model = model
		self.attr = kwargs
		print("Class %s created with attributes %s" % (model, str(kwargs)))

	def insert(self, **kwargs):
		query = "INSERT INTO"
		s = "("
		for keys in kwargs.keys():
			if keys in self.attr.keys():
				s += "%s, " % keys
			else:
				print("Essa classe não tem esse atributo: %s" % keys)
				raise AttributeError
		s = s[:-2] + ")"
		for k in self.attr.keys():
			if self.attr[k] and (k not in kwargs.keys()):
				print("Você não incluiu esse campo obrigatório: %s" % k)
				raise AttributeError
		val = ""
		for v in kwargs.values():
			val += "\'%s\', " % v
		query += " %s %s " % (self.model, s)
		query += "VALUES (%s)" % val[:-2]
		try:
			self.conn.cur.execute(query)
		except Exception as e:
			print("It wasn't possible to run such transaction")
			print(e)
			self.conn.rollback()
		finally:
			self.conn.commit()

	def queries(self, query):
		if not query:
			self.conn.cur.execute("SELECT * FROM %s", [self.model])
			return self.conn.cur.fetchmany()
		else:
			try:
				self.conn.cur.execute(query)
			except Exception as e:
				print("It wasn't possible to run such query")
				print(e)
			finally:
				return self.conn.cur.fetchmany()

	def update(self):
		pass

	def delete(self, **kwargs):
		del_string = "DELETE FROM %s" % self.model
		if kwargs:
			del_string += " WHERE "
			for arg in kwargs:
				del_string += "%s = %s, " % (arg, kwargs[arg])
			del_string = del_string[:-2]

		try:
			self.conn.cur.execute(del_string)
		except Exception as e:
			print("It wasn't possible to delete it")
			self.conn.rollback()
			print(e)
		finally:
			self.conn.commit()
