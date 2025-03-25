SELECT Student.B, Student.C, SUM(1), SUM(Student.A) FROM Student, Enrolled GROUP BY Student.B, Student.C;
-- SELECT Student.B FROM Student, Enrolled WHERE Student.A = Enrolled.A GROUP BY Student.B, Student.C ORDER BY Student.B;
-- SELECT Enrolled.E, SUM(2 * Enrolled.H * Enrolled.E), SUM(Enrolled.F) FROM Enrolled GROUP BY Enrolled.E, Enrolled.H;
-- SELECT Student.B, Student.C FROM Student, Enrolled WHERE Student.A = Enrolled.A GROUP BY Student.B, Student.C ORDER BY Student.C, Student.B;
-- SELECT SUM(1), SUM(Student.A) FROM Student, Enrolled;
-- SELECT SUM(1) FROM Student GROUP BY Student.B;
-- SELECT Enrolled.E, SUM(2 * Enrolled.H * Enrolled.E) FROM Enrolled GROUP BY Enrolled.E;