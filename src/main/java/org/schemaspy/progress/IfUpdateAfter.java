package org.schemaspy.progress;

import java.time.Duration;
import java.time.Instant;
import java.time.Clock;

/**
 * Implementation of a Condition that only
 * returns true if a certain amount of time
 * has passed since the previous reporting.
 */

public class IfUpdateAfter implements Condition {

  private final Clock clock;
  private final Duration updateFrequency;
  private Instant nextUpdate;

  public IfUpdateAfter(
      final Duration updateFrequency,
      final Clock clock
  ) {
    this.updateFrequency = updateFrequency;
    this.clock = clock;
    this.nextUpdate = this.clock
        .instant()
        .plus(this.updateFrequency);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean report() {
    final Instant now = this.clock.instant();
    if (now.isAfter(this.nextUpdate)) {
      this.nextUpdate = now.plus(this.updateFrequency);
      return true;
    }
    return false;
  }

}
