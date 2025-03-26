package ed.inf.adbs.blazedb;

import java.util.*;

/**
 * The Tuple class represents a row of data.
 * Each Tuple object contains a list of integer values.
 * It provides methods to access the values and to compare Tuples.
 *
 * A brief description of the methods is provided below:
 * getValue(int index) returns the value at the specified index.
 * getValues() returns an unmodifiable list of values.
 * toString() returns a string representation of the Tuple.
 * equals(Object o) checks if the Tuple is equal to another object.
 * hashCode() returns the hash code of the Tuple.
 *
 * With the last 3 methods overridden, the Tuple class can be used in collections like HashMap and HashSet.
 */

public class Tuple {
    private List<Integer> values;

    public Tuple() {}

    public Tuple(List<Integer> values) {
        this.values = new ArrayList<>(values);
    }

    public Integer getValue(int index) {
        return values.get(index);
    }

    public List<Integer> getValues() {
        return Collections.unmodifiableList(values);
    }

    @Override
    public String toString() {
        return String.join(", ", values.stream().map(String::valueOf).toArray(String[]::new));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Tuple tuple = (Tuple) o;
        return Objects.equals(values, tuple.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(values);
    }
}
