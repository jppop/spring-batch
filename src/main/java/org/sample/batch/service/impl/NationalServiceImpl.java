package org.sample.batch.service.impl;

import org.sample.batch.service.NationalService;

import java.util.Optional;

public class NationalServiceImpl implements NationalService {

  @Override
  public Optional<String> findNationalIdentifier(String firstName, String lastName) {
    return Optional.empty();
  }
}
