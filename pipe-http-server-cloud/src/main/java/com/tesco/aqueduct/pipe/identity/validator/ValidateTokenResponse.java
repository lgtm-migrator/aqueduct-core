package com.tesco.aqueduct.pipe.identity.validator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.tesco.aqueduct.pipe.logger.PipeLogger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class ValidateTokenResponse {
    private static final PipeLogger LOG = new PipeLogger(LoggerFactory.getLogger(ValidateTokenResponse.class));
    private static final int SERVICE_CONFIDENCE_LEVEL = 12;

    private final String userId;
    private final String status;
    private final Collection<Claim> claims;

    private int confidenceLevel = 0;

    static class Claim {
        private static final String CONFIDENCE_LEVEL_CLAIM = "http://schemas.tesco.com/ws/2011/12/identity/claims/confidencelevel";
        private static final String FORMER_USER_KEY_CLAIM = "http://schemas.tesco.com/ws/2018/05/identity/claims/formeruserkeys";
        private static final String MERGED_CLAIM = "http://schemas.tesco.com/ws/2011/12/identity/claims/merged";

        private String claimType, value;
        private Collection<Values> values;

        @JsonCreator
        Claim(
                @JsonProperty(value = "claimType", required = true) String claimType,
                @JsonProperty(value = "value") String value,
                @JsonProperty(value = "values") Collection<Values> values
        ) {
            this.claimType = claimType;
            this.value = value;
            this.values = values;
        }

        boolean isForConfidenceLevel(){
            return this.claimType.equals(CONFIDENCE_LEVEL_CLAIM);
        }
    }

    static class Values{
        private String uuid;

        @JsonCreator
        Values(
                @JsonProperty(value="uuid") String uuid
        ){
            this.uuid = uuid;
        }
    }

    static class MultiKeyValues extends Values{
        private String uuidType;

        MultiKeyValues(
                @JsonProperty(value="uuid") String uuid,
                @JsonProperty(value="uuidType") String uuidType
        ){
            super(uuid);
            this.uuidType = uuidType;
        }
    }

    @JsonCreator
    ValidateTokenResponse(
            @JsonProperty(value = "UserId") String userId,
            @JsonProperty(value = "Status", required = true) String status,
            @JsonProperty(value = "Claims") Collection<Claim> claims
    ) {
        this.userId = userId;
        this.status = status;
        this.claims = claims;

        if (claims != null) {
            this.confidenceLevel = determineConfidenceLevel();
        }
    }

    private int determineConfidenceLevel() {
        return claims.stream()
                .filter(Claim::isForConfidenceLevel)
                .mapToInt(claim -> Integer.parseInt(claim.value))
                .findFirst()
                .orElse(0);
    }

    boolean isTokenValid() {
        boolean isValid = status.equals("VALID") && confidenceLevel == SERVICE_CONFIDENCE_LEVEL;
        LOG.info("is token valid", String.valueOf(isValid));
        return isValid;
    }

    String getClientUserID() {
        return this.userId;
    }
}