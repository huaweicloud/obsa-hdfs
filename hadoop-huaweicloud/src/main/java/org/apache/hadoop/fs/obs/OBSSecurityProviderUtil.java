package org.apache.hadoop.fs.obs;

import com.obs.services.BasicObsCredentialsProvider;
import com.obs.services.IObsCredentialsProvider;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.Optional;

public class OBSSecurityProviderUtil {

    /**
     * Class logger.
     */
    private static final Logger LOG = LoggerFactory.getLogger(OBSSecurityProviderUtil.class);

    public static IObsCredentialsProvider createObsSecurityProvider(final Configuration conf, final URI name)
        throws IOException {
        Class<?> credentialsProviderClass;
        BasicSessionCredential credentialsProvider;

        try {
            credentialsProviderClass = conf.getClass(OBSConstants.OBS_CREDENTIALS_PROVIDER, null);
        } catch (RuntimeException e) {
            Throwable c = e.getCause() != null ? e.getCause() : e;
            throw new IOException("From option " + OBSConstants.OBS_CREDENTIALS_PROVIDER + ' ' + c, c);
        }

        if (credentialsProviderClass == null) {
            return innerCreateObsSecurityProvider(conf, name);
        }

        try {
            Constructor<?> cons = credentialsProviderClass.getDeclaredConstructor(URI.class, Configuration.class);
            credentialsProvider = (BasicSessionCredential) cons.newInstance(name, conf);
        } catch (NoSuchMethodException | SecurityException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
            Throwable c = e.getCause() != null ? e.getCause() : e;
            throw new IOException("From option " + OBSConstants.OBS_CREDENTIALS_PROVIDER + ' ' + c, c);
        }

        LOG.info("create ObsClient using credentialsProvider: {}", credentialsProviderClass.getName());
        String sessionToken = credentialsProvider.getSessionToken();
        String ak = credentialsProvider.getOBSAccessKeyId();
        String sk = credentialsProvider.getOBSSecretKey();
        if (sessionToken != null && sessionToken.length() != 0) {
            return new BasicObsCredentialsProvider(ak, sk, sessionToken);
        } else {
            return new BasicObsCredentialsProvider(ak, sk);
        }
    }

    private static IObsCredentialsProvider innerCreateObsSecurityProvider(final Configuration conf,
        final URI name) throws IOException {
        OBSLoginHelper.Login creds = OBSCommonUtils.getOBSAccessKeys(name, conf);

        String ak = creds.getUser();
        String sk = creds.getPassword();
        String token = creds.getToken();

        if (OBSCommonUtils.isStringNotEmpty(ak) || OBSCommonUtils.isStringNotEmpty(sk)) {
            LOG.info("create ObsClient using aksk from configuration");
            return new BasicObsCredentialsProvider(ak, sk, token);
        }

        Class<?> securityProviderClass;
        try {
            securityProviderClass = conf.getClass(OBSConstants.OBS_SECURITY_PROVIDER, null);
            LOG.info("From option {} get {}", OBSConstants.OBS_SECURITY_PROVIDER, securityProviderClass);
        } catch (RuntimeException e) {
            Throwable c = e.getCause() != null ? e.getCause() : e;
            throw new IOException("From option " + OBSConstants.OBS_SECURITY_PROVIDER + ' ' + c, c);
        }

        if (securityProviderClass == null) {
            LOG.info("create ObsClient when securityProviderClass is null");
            return new BasicObsCredentialsProvider(ak, sk, token);
        }

        LOG.info("create ObsClient using securityProvider {}", securityProviderClass.getName());
        IObsCredentialsProvider securityProvider;
        try {
            Optional<Constructor<?>> cons = tryGetConstructor(securityProviderClass,
                new Class[] {URI.class, Configuration.class});

            if (cons.isPresent()) {
                securityProvider = (IObsCredentialsProvider) cons.get().newInstance(name, conf);
            } else {
                securityProvider = (IObsCredentialsProvider) securityProviderClass.getDeclaredConstructor()
                    .newInstance();
            }

        } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException | RuntimeException e) {
            Throwable c = e.getCause() != null ? e.getCause() : e;
            throw new IOException("From option " + OBSConstants.OBS_SECURITY_PROVIDER + ' ' + c, c);
        }
        return securityProvider;
    }

    private static Optional<Constructor<?>> tryGetConstructor(final Class<?> mainClss, final Class<?>[] args) {
        try {
            Constructor<?> constructor = mainClss.getDeclaredConstructor(args);
            return Optional.ofNullable(constructor);
        } catch (NoSuchMethodException e) {
            // ignore
            return Optional.empty();
        }
    }
}
