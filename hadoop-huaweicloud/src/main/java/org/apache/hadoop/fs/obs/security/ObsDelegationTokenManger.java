package org.apache.hadoop.fs.obs.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.obs.OBSCommonUtils;
import org.apache.hadoop.fs.obs.OBSConstants;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * description
 *
 * @since 2022-05-24
 */
public class ObsDelegationTokenManger {
    private static final Logger logger = LoggerFactory.getLogger(ObsDelegationTokenManger.class);
    private List<DelegationTokenProvider> providers = new ArrayList<>();
    private DelegationTokenProvider defaultProvider;

    public static boolean hasDelegationTokenProviders(Configuration conf) {
        return UserGroupInformation.isSecurityEnabled() && OBSCommonUtils.isStringNotEmpty(
            conf.getTrimmed(OBSConstants.DELEGATION_TOKEN_PROVIDERS, OBSConstants.DEFAULT_DELEGATION_TOKEN_PROVIDERS));
    }

    public void initialize(FileSystem fs, URI uri, Configuration conf) throws IOException {
        List<Class<?>> providerClasses = Arrays.asList(
            conf.getClasses(OBSConstants.DELEGATION_TOKEN_PROVIDERS, new Class[] {}));

        for (int i = 0; i < providerClasses.size(); i++) {
            DelegationTokenProvider provider = null;
            try {
                provider = (DelegationTokenProvider)providerClasses.get(i).newInstance();
            } catch (InstantiationException | IllegalAccessException e) {
                throw new IOException("Instantiation DelegationTokenProvider", e);
            }
            provider.initialize(fs, uri, conf);
            if (i == 0) {
                this.defaultProvider = provider;
            }
            this.providers.add(provider);
        }
    }

    public Token<?>[] addDelegationTokens(String renewer, Credentials credentials) {
        List<Token<?>> tokens = new ArrayList<>();
        for (DelegationTokenProvider provider : providers) {
            final String serviceName = provider.getCanonicalServiceName();
            logger.info("get delegation token from provider {} with serviceName {}",
                provider.getClass().getName(), serviceName);
            if (serviceName != null) {
                final Text service = new Text(serviceName);
                Token<?> token = credentials.getToken(service);
                if (token == null) {
                    token = provider.getDelegationToken(renewer);
                    if (token != null) {
                        tokens.add(token);
                        credentials.addToken(service, token);
                    }
                }
            }
        }
        return tokens.toArray(new Token<?>[tokens.size()]);
    }

    public Token<?> getDelegationToken(String renewer) {
        return defaultProvider.getDelegationToken(renewer);
    }

    public String getCanonicalServiceName() {
        return defaultProvider.getCanonicalServiceName();
    }
}
