// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.core;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

/**
 * Utility class that provides common, generally useful aggregators.
 */
public final class Aggregators {
  //private static final Logger LOG = LoggerFactory.getLogger(Aggregators.class);

  /** Aggregator that sums up all the data points. */
  public static final Aggregator SUM = new Sum("sum", true);
  /** Aggregator that sums only explicit data points and does no interpolation */
  public static final Aggregator ESUM = new Sum("esum", false);

  /** Aggregator that returns the minimum data point. */
  public static final Aggregator MIN = new Min("min", true);

  /** Aggregator that returns the maximum data point. */
  public static final Aggregator MAX = new Max("max", true);

  /** Aggregator that returns the average value of all the data points. */
  public static final Aggregator AVG = new Avg("avg", true);
  /** Aggregator that returns the average value of only explicit data points and does no interpolation. */
  public static final Aggregator EAVG = new Avg("eavg", false);

  /** Aggregator that returns the Standard Deviation of the data points. */
  public static final Aggregator DEV = new StdDev("dev", true);
  /** Aggregator that returns the Standard Deviation of only explicit data points and does no interpolation. */
  public static final Aggregator EDEV = new StdDev("edev", false);

  /** Aggregator that counts only explicit data points and does no interpolation */
  public static final Aggregator COUNT = new Count("count", false);


  /** Maps an aggregator name to its instance. */
  private static final HashMap<String, Aggregator> aggregators;

  static {
    aggregators = new HashMap<String, Aggregator>(9);
    aggregators.put(SUM.getName(), SUM);
    aggregators.put(MIN.getName(), MIN);
    aggregators.put(MAX.getName(), MAX);
    aggregators.put(AVG.getName(), AVG);
    aggregators.put(DEV.getName(), DEV);
    aggregators.put(ESUM.getName(), ESUM);
    aggregators.put(EAVG.getName(), EAVG);
    aggregators.put(EDEV.getName(), EDEV);
    aggregators.put(COUNT.getName(), COUNT);
    //for (final Map.Entry<String, Aggregator> agg : aggregators.entrySet()) {
    //    LOG.debug("Adding aggregator: " + agg.getKey());
    //}
  }

  private Aggregators() {
    // Can't create instances of this utility class.
  }

  /**
   * Returns the set of the names that can be used with {@link #get get}.
   */
  public static Set<String> set() {
    return aggregators.keySet();
  }

  /**
   * Returns the aggregator corresponding to the given name.
   * @param name The name of the aggregator to get.
   * @throws NoSuchElementException if the given name doesn't exist.
   * @see #set
   */
  public static Aggregator get(final String name) {
    final Aggregator agg = aggregators.get(name);
    if (agg != null) {
      return agg;
    }
    throw new NoSuchElementException("No such aggregator: " + name);
  }

  private static class BaseAggregator {
    /* Whether or not to interpolate values for this aggregation. */
    private boolean do_interpolation;

    public boolean interpolate() {
        return do_interpolation;
    }

    /* Name of the aggregator. */
    private String name;

    public String getName() {
        return name;
    }

    public String toString() {
      return getName();
    }

    BaseAggregator(final String name, final boolean interpolate) {
        this.name = name;
        this.do_interpolation = interpolate;
    }
  }

  private static final class Sum extends BaseAggregator implements Aggregator {
    //private final Logger LOG = LoggerFactory.getLogger(Sum.class);

    Sum(final String name, final boolean interpolate) {
        super(name, interpolate);
    }

    public long runLong(final Longs values) {
      long result = values.nextLongValue();
      //LOG.debug("runLong first value: " + result); 
      while (values.hasNextValue()) {
        long next = values.nextLongValue();
        //LOG.debug("runLong: " + result  + " + " + next + " = " + (result+next));
        result += next;
      }
      return result;
    }

    public double runDouble(final Doubles values) {
      double result = values.nextDoubleValue();
      //LOG.debug("runDouble first value: " + result); 
      while (values.hasNextValue()) {
        double next = values.nextDoubleValue();
        //LOG.debug("runDouble: " + result  + " + " + next + " = " + (result+next));
        result += next;
      }
      return result;
    }

  }

  private static final class Count extends BaseAggregator implements Aggregator {

    Count(final String name, final boolean interpolate) {
        super(name, interpolate);
    }

    public long runLong(final Longs values) {
      int count = 0;
      while (values.hasNextValue()) {
        values.nextLongValue();
        count++;
      }
      return count;
    }

    public double runDouble(final Doubles values) {
      int count = 0;
      while (values.hasNextValue()) {
        values.nextDoubleValue();
        count++;
      }
      return count;
    }
  }

  private static final class Min extends BaseAggregator implements Aggregator {

    Min(final String name, final boolean interpolate) {
        super(name, interpolate);
    }

    public long runLong(final Longs values) {
      long min = values.nextLongValue();
      while (values.hasNextValue()) {
        final long val = values.nextLongValue();
        if (val < min) {
          min = val;
        }
      }
      return min;
    }

    public double runDouble(final Doubles values) {
      double min = values.nextDoubleValue();
      while (values.hasNextValue()) {
        final double val = values.nextDoubleValue();
        if (val < min) {
          min = val;
        }
      }
      return min;
    }

  }

  private static final class Max extends BaseAggregator implements Aggregator {

    Max(final String name, final boolean interpolate) {
        super(name, interpolate);
    }

    public long runLong(final Longs values) {
      long max = values.nextLongValue();
      while (values.hasNextValue()) {
        final long val = values.nextLongValue();
        if (val > max) {
          max = val;
        }
      }
      return max;
    }

    public double runDouble(final Doubles values) {
      double max = values.nextDoubleValue();
      while (values.hasNextValue()) {
        final double val = values.nextDoubleValue();
        if (val > max) {
          max = val;
        }
      }
      return max;
    }

  }

  private static final class Avg extends BaseAggregator implements Aggregator {

    Avg(final String name, final boolean interpolate) {
        super(name, interpolate);
    }

    public long runLong(final Longs values) {
      long result = values.nextLongValue();
      int n = 1;
      while (values.hasNextValue()) {
        result += values.nextLongValue();
        n++;
      }
      return result / n;
    }

    public double runDouble(final Doubles values) {
      double result = values.nextDoubleValue();
      int n = 1;
      while (values.hasNextValue()) {
        result += values.nextDoubleValue();
        n++;
      }
      return result / n;
    }

  }

  /**
   * Standard Deviation aggregator.
   * Can compute without storing all of the data points in memory at the same
   * time.  This implementation is based upon a
   * <a href="http://www.johndcook.com/standard_deviation.html">paper by John
   * D. Cook</a>, which itself is based upon a method that goes back to a 1962
   * paper by B.  P. Welford and is presented in Donald Knuth's Art of
   * Computer Programming, Vol 2, page 232, 3rd edition
   */
  private static final class StdDev extends BaseAggregator implements Aggregator {

    StdDev(final String name, final boolean interpolate) {
        super(name, interpolate);
    }

    public long runLong(final Longs values) {
      double old_mean = values.nextLongValue();

      if (!values.hasNextValue()) {
        return 0;
      }

      long n = 2;
      double new_mean = 0;
      double variance = 0;
      do {
        final double x = values.nextLongValue();
        new_mean = old_mean + (x - old_mean) / n;
        variance += (x - old_mean) * (x - new_mean);
        old_mean = new_mean;
        n++;
      } while (values.hasNextValue());

      return (long) Math.sqrt(variance / (n - 1));
    }

    public double runDouble(final Doubles values) {
      double old_mean = values.nextDoubleValue();

      if (!values.hasNextValue()) {
        return 0;
      }

      long n = 2;
      double new_mean = 0;
      double variance = 0;
      do {
        final double x = values.nextDoubleValue();
        new_mean = old_mean + (x - old_mean) / n;
        variance += (x - old_mean) * (x - new_mean);
        old_mean = new_mean;
        n++;
      } while (values.hasNextValue());

      return Math.sqrt(variance / (n - 1));
    }

  }
}
