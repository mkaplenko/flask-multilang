__author__ = 'mkaplenko'
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import TSVECTOR
from sqlalchemy.orm import mapper, object_mapper, relationship
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.schema import DDL
from brpr_admin import dontworry
from class_property.descriptor import classproperty
from sqlalchemy.orm.query import Query


class DBLangColumn(sa.Column):
    def __init__(self, *args, **kwargs):
        self.tsweight = kwargs.pop('tsweight', None)
        super(DBLangColumn, self).__init__(*args, **kwargs)


class LanguageColumn(object):
    def __init__(self, *args, **kwargs):
        self.orm_column = args[0]
        self.tsweight = kwargs.pop('tsweight', None)


class SearchMapperCreator(object):
    def __init__(self, lang_mapper_creator):
        self.lang_mapper_creator = lang_mapper_creator

    def __construct_table(self):
        columns = []
        columns.append(sa.Column('id', sa.Integer(), primary_key=True))
        columns.append(sa.Column('search_vector', TSVECTOR))
        columns.append(
            sa.Column(self.lang_mapper_creator.localmapper_class.__lang_mapper__.local_table.name + '_id',
                      sa.ForeignKey(self.lang_mapper_creator.localmapper_class.__lang_mapper__.local_table.columns.id)
            )
        )  # Add ForignKey to base table column
        table = sa.Table(self.lang_mapper_creator.local_mapper.local_table.name+'_search_fields', self.lang_mapper_creator.local_mapper.local_table.metadata, *columns, schema=self.lang_mapper_creator.local_mapper.local_table.schema)

        table.indexes.update([sa.Index('{0}_tsvector'.format(table.name), table.c.search_vector, postgresql_using="gin")])
        return table

    def create_search_mapper(self):
        orm_class = self.create_search_class()
        search_mapper = mapper(orm_class, self.__construct_table(),
                             properties={'lang_fields': relationship(self.lang_mapper_creator.localmapper_class.__lang_mapper__,
                                                              uselist=False,
                                                              backref=sa.orm.backref('search_fields',
                                                                                     uselist=True,
                                                                                     lazy='joined',
                                                                                     cascade='all, delete-orphan'
                                                                                     )
                                                              )
                                         }
                             )
        return search_mapper

    def create_search_class(self):
        new_class = type.__new__(type,
                                 '%sSearchable' % self.lang_mapper_creator.localmapper_class.__name__,
                                 self.lang_mapper_creator.local_mapper.base_mapper.class_.__bases__[1:2], {}
        )
        return new_class


class LangMapperCreator(object):
    def __init__(self, local_mapper):
        self.local_mapper = local_mapper
        self.search_mapper_creator = SearchMapperCreator(self)

    @staticmethod
    def lang_objects(sequence):
        for obj in sequence:
            if hasattr(obj, '__lang_mapper__'):
                yield obj

    @property
    def localmapper_class(self):
        return self.local_mapper.class_

    @property
    def lang_class(self):
        _class = type.__new__(type, '%sLanguaged' % self.localmapper_class.__name__, self.local_mapper.base_mapper.class_.__bases__[1:2], {})
        return _class

    def __construct_table(self, tablename, columns, schema, extends=False):
        table = sa.Table(tablename, self.local_mapper.local_table.metadata, *columns, schema=schema, extend_existing=extends)

        return table

    def create_lang_mapper(self):
        columns = []
        columns.append(sa.Column('id', sa.Integer(), primary_key=True))  # Add primary as a first column of new table
        columns.append(sa.Column('lang', sa.String()))

        for attr in self.localmapper_class.__dict__:
            q_lang_column = self.localmapper_class.__dict__[attr]
            if isinstance(q_lang_column, LanguageColumn):
                columns.append(DBLangColumn(attr, q_lang_column.orm_column, tsweight=q_lang_column.tsweight))

        # columns.append(sa.Column('search_vector', sa.dialects.postgresql.TSVECTOR))  # Add TSVECTOR column as last column of langed table

        columns.append(
            sa.Column(self.local_mapper.local_table.name + '_id',
                      sa.ForeignKey(self.local_mapper.local_table.columns.id)
            )
        )  # Add ForignKey to base table column

        table = self.__construct_table(self.local_mapper.local_table.name+'_lang_fields', columns,
                                       self.local_mapper.local_table.schema)

        #  Add GIN index to TSVECTOR column
        # table.indexes.update([sa.Index('{0}_tsvector'.format(table.name), table.c.search_vector, postgresql_using="gin")])

        lang_mapper = mapper(self.lang_class, table,
                             properties={'base': relationship(self.local_mapper,
                                                              uselist=False,
                                                              backref=sa.orm.backref('lang_fields',
                                                                                     uselist=True,
                                                                                     lazy='joined',
                                                                                     cascade='all, delete-orphan'
                                                                                     )
                                                              )
                                         }
                             )


        self.localmapper_class.__lang_mapper__ = lang_mapper

        # Create searchable mapper after lang mapper
        search_mapper = self.search_mapper_creator.create_search_mapper()
        self.localmapper_class.__search_mapper__ = search_mapper


class LangQueryManager(object):
    def __init__(self, model, *args, **kwargs):
        self.model = model
        # Query.__init__(self, *args, **kwargs)

    def filter(self, *criterion):
        print(list(criterion))

    def filter_by(self, **kwargs):
        # print(self.model)
        obvious_filters = dict()
        lang_filters = dict()
        for field_name in kwargs:
            if field_name != 'lang':
                if isinstance(self.model.__dict__[field_name], LanguageColumn):
                    lang_filters[field_name] = kwargs[field_name]
                else:
                    obvious_filters[field_name] = kwargs[field_name]

        if 'lang' in kwargs:
            lang_filters.update({'lang': kwargs['lang']})
            self.model.lang = kwargs['lang']

        # print(self.model.base_query())
        result = self.model.base_query().filter_by(**obvious_filters)  # self.model.query.filter_by(**obvious_filters)
        result = result.filter(self.model.lang_fields.any(**lang_filters))
        print('RESULT = ', result.all())
        return result


class MultiLanguage(object):
    lang = 'ru'

    def __init__(self, *args, **kwargs):
        self.lang = kwargs.pop('lang', None)
        super(MultiLanguage, self).__init__(*args, **kwargs)

    def __getattribute__(self, item):
        attr = super(MultiLanguage, self).__getattribute__(item)
        if isinstance(attr, LanguageColumn):
            for lang_field in self.lang_fields:
                if lang_field.lang == self.lang:
                    return getattr(lang_field, item)
        return attr

    @classproperty
    def query(cls, *args, **kwargs):
        return LangQueryManager(cls, *args, **kwargs)

    @classmethod
    def base_query(cls):
        return super(MultiLanguage, cls).query

    @property
    def mapper_creator(self):
        mc = LangMapperCreator(object_mapper(instance=self))
        return mc

    def create_lang_fields(self, session):
        lang_table = self.__lang_mapper__.class_()
        lang_table.base = self
        instance_mapper = object_mapper(lang_table)
        search_vectors = []
        for column in instance_mapper.columns:
            if isinstance(column, DBLangColumn):
                setattr(lang_table, column.name, getattr(self, column.name))
                # print(column.name, getattr(self, column.name))
                if column.tsweight:
                    search_vectors.append(sa.func.setweight(sa.func.to_tsvector(unicode(getattr(self, column.name))), column.tsweight))

        # SET LANGUAGE
        setattr(lang_table, 'lang', self.lang)

        #UPDATE SEARCH FIELDS
        search_table = self.__search_mapper__.class_()
        search_table.lang_fields = lang_table
        for count, vector in enumerate(search_vectors):
            if not count:
                search_table.search_vector = vector + ' '
            else:
                search_table.search_vector = search_table.search_vector + vector + ' '

        session.add(lang_table)
        session.add(search_table)

    def write_lang_fields(self, **kwargs):
        self.lang = kwargs['lang']
        exist = None
        for lang_fields_item in self.lang_fields:
            if lang_fields_item.lang == self.lang:
                exist = lang_fields_item

        if exist:
            for field in kwargs:
                setattr(exist, field, kwargs[field])

        else:
            new_lang_fields = self.__lang_mapper__.class_(**kwargs)
            tsvectors = []
            for column in self.__lang_mapper__.columns:
                if isinstance(column, DBLangColumn):
                    setattr(new_lang_fields, column.name, kwargs.get(column.name))
                    if column.tsweight:
                        tsvectors.append(sa.func.setweight(sa.func.to_tsvector(unicode(kwargs.get(column.name))), column.tsweight))

            new_search_fields = self.__search_mapper__.class_()

            for count, vector in enumerate(tsvectors):
                if not count:
                    new_search_fields.search_vector = vector + ' '
                else:
                    new_search_fields.search_vector = new_search_fields.search_vector + vector + ' '

            new_lang_fields.search_fields.append(new_search_fields)
            self.lang_fields.append(new_lang_fields)

    @declared_attr
    def __mapper_cls__(self):
        def map(cls, *args, **kwargs):
            mp = mapper(cls, *args, **kwargs)
            LangMapperCreator(mp).create_lang_mapper()
            return mp
        return map


def session_handler(session):
    @sa.event.listens_for(session, 'before_flush')
    def before_flush(session, context, instances):
        # print('BEFORE_FLUSH')
        for instance in LangMapperCreator.lang_objects(session.new):
            LangMapperCreator(object_mapper(instance=instance))
            instance.create_lang_fields(session)
    return before_flush