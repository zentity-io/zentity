package org.elasticsearch.plugin.zentity;

import org.elasticsearch.ElasticsearchSecurityException;

class ForbiddenException extends ElasticsearchSecurityException {
  public ForbiddenException(String message) {
    super(message);
  }
}
