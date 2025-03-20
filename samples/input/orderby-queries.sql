SELECT * FROM Student ORDER BY Student.B;
SELECT * FROM Student ORDER BY Student.B, Student.D;
SELECT Student.D, Student.B, Student.C FROM Student ORDER BY Student.B, Student.D;
SELECT Student.D, Student.B, Student.C FROM Student WHERE Student.B = 100 ORDER BY Student.B, Student.D;
SELECT * FROM Student, Enrolled WHERE Student.A = Enrolled.A ORDER BY Student.B, Enrolled.H;