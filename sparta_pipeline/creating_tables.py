from sqlalchemy import *


file = open("credentials.txt")
all_lines = file.readlines()
converted = []
for element in all_lines:
    converted.append(element.strip())
file.close()

server = converted[0]
database = converted[1]
user = converted[2]
password = converted[3]
driver = converted[4]
engine = create_engine(f"mssql+pyodbc://{user}:{password}@{server}/{database}?driver={driver}")

connection = engine.connect()
meta = MetaData()


def create_courses():
    Table(
        'courses', meta,
        Column('course_id', Integer, primary_key=True),
        Column('name', String),
    )


def create_classes():
    Table(
        'classes', meta,
        Column('class_id', Integer, primary_key=True),
        Column('name_number', String),
    )


def create_student_information():
    Table(
        'student_information', meta,
        Column('student_id', Integer, primary_key=True),
        Column('name', String),
        Column('date', Date),
        Column('self_development', Boolean),
        Column('geo_flex', Boolean),
        Column('financial_support_self', Boolean),
        Column('result', Boolean),
        Column('course_id', Integer, ForeignKey("courses.course_id")),
        Column('class_id', Integer, ForeignKey("classes.class_id"))
    )


def create_behaviours():
    Table(
        'behaviour_types', meta,
        Column('behaviour_id', Integer, primary_key=True),
        Column('behaviour', String)
    )


def create_weeks():
    Table(
        'weeks', meta,
        Column('student_id', Integer, ForeignKey("student_information.student_id")),
        Column('week_id', Integer),
        Column('behaviour_id', Integer, ForeignKey("behaviour_types.behaviour_id")),
        Column('score', Integer)
    )


def create_techs():
    Table(
        'tech_types', meta,
        Column('tech_id', Integer, primary_key=True),
        Column('name', String)
    )


def create_self_score():
    Table(
        'self_score', meta,
        Column('student_id', Integer, ForeignKey("student_information.student_id")),
        Column('tech_id', Integer, ForeignKey("tech_types.tech_id")),
        Column('score', Integer)
    )


def create_strengths():
    Table(
        'strength_types', meta,
        Column('strength_id', Integer, primary_key=True),
        Column('name', String)
    )


def create_student_strengths():
    Table(
        'student_strengths', meta,
        Column('student_id', Integer, ForeignKey("student_information.student_id")),
        Column('strength_id', Integer, ForeignKey("strength_types.strength_id"))
    )


def create_weaknesses():
    Table(
        'weakness_types', meta,
        Column('weakness_id', Integer, primary_key=True),
        Column('name', String)
    )


def create_student_weaknesses():
    Table(
        'student_weaknesses', meta,
        Column('student_id', ForeignKey("student_information.student_id")),
        Column('weakness_id', ForeignKey("weakness_types.weakness_id"))
    )


def create_scores():
    Table(
        'scores', meta,
        Column('student_id', Integer, primary_key=True),
        Column('psychometrics_score', Integer),
        Column('presentations_score', Integer),
        Column('psycometrics_max', Integer),
        Column('presentations_max', Integer)
    )


def create_personal_information():
    Table(
        'personal_information', meta,
        Column('student_id', Integer, primary_key=True),
        Column('name', String),
        Column('invited_date', Date),
        Column('gender', String),
        Column('date_of_birth', Date),
        Column('email', String),
        Column('city', String),
        Column('address', String),
        Column('postcode', String),
        Column('phone_number', String),
        Column('university', String),
        Column('degree', String),
        Column('month', String),
        Column('invited_by', String)
    )


def main():
    create_courses()
    create_classes()
    create_student_information()
    create_behaviours()
    create_weeks()
    create_techs()
    create_self_score()
    create_strengths()
    create_student_strengths()
    create_weaknesses()
    create_student_weaknesses()
    create_scores()
    create_personal_information()
    meta.create_all(engine)


if __name__ == "__main__":
    main()
