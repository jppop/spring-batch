package org.sample.batch.service;

import java.util.Optional;

public interface NationalService {

  Optional<String> findNationalIdentifier(String firstName, String lastName);
}
