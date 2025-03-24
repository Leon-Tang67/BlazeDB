SELECT * FROM Student, Enrolled WHERE Student.A = Enrolled.A;
SELECT * FROM Student, Enrolled WHERE Student.A = Enrolled.A AND Student.B < Enrolled.E;
SELECT * FROM Student, Enrolled, Course WHERE Student.A = Enrolled.A AND Student.B < Enrolled.E;
SELECT Enrolled.E, Student.B FROM Student, Enrolled WHERE Student.A = Enrolled.A AND Student.B < Enrolled.E;
SELECT Enrolled.E, Student.B, Course.E FROM Student, Enrolled, Course WHERE Student.A = Enrolled.A AND Student.B < Enrolled.E;
