SELECT Enrolled.E, SUM(Enrolled.H * Enrolled.H) FROM Enrolled GROUP BY Enrolled.E;
SELECT SUM(1) FROM Student GROUP BY Student.B;
SELECT Student.B, Student.C FROM Student, Enrolled WHERE Student.A = Enrolled.A GROUP BY Student.B, Student.C ORDER BY Student.C, Student.B;
SELECT SUM(1), SUM(Student.A) FROM Student, Enrolled;