package org.apache.hadoop.fs.obs.memartscc;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.TokenIdentifier;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MemArtsCCDelegationTokenIdentifier extends TokenIdentifier {
    public static final Text MEMARTSCC_DELEGATION_KIND =
        new Text("MEMARTSCC_DELEGATION_TOKEN");

    private Text owner;
    private Text renewer;
    private Text realUser;

    public MemArtsCCDelegationTokenIdentifier() {
        owner = new Text();
        renewer = new Text();
        realUser = new Text();
    }


    public MemArtsCCDelegationTokenIdentifier(Text owner, Text renewer, Text realUser) {
        setOwner(owner);
        setRenewer(renewer);
        setRealUser(realUser);
    }

    public MemArtsCCDelegationTokenIdentifier(Text renewer) {
        this(null, renewer, null);
    }

    @Override
    public Text getKind() {
        return MEMARTSCC_DELEGATION_KIND;
    }

    @Override
    public UserGroupInformation getUser() {
        if(owner == null) {
            return null;
        }
        if(owner.toString().isEmpty()) {
            return null;
        }
        final UserGroupInformation realUgi;
        final UserGroupInformation ugi;
        if ((realUser == null) || (realUser.toString().isEmpty())
            || realUser.equals(owner)) {
            realUgi = UserGroupInformation.createRemoteUser(owner.toString());
            ugi = realUgi;
        } else {
            realUgi = UserGroupInformation.createRemoteUser(realUser.toString());
            ugi = UserGroupInformation.createProxyUser(owner.toString(), realUgi);
        }
        realUgi.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.TOKEN);
        return ugi;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        owner.write(dataOutput);
        renewer.write(dataOutput);
        realUser.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        owner.readFields(dataInput, Text.DEFAULT_MAX_LEN);
        renewer.readFields(dataInput, Text.DEFAULT_MAX_LEN);
        realUser.readFields(dataInput, Text.DEFAULT_MAX_LEN);
    }

    public Text getOwner() {
        return owner;
    }

    private void setOwner(Text owner) {
        if (owner == null) {
            this.owner = new Text();
        } else {
            this.owner = owner;
        }
    }

    public Text getRenewer() {
        return renewer;
    }

    private void setRenewer(Text renewer) {
        this.renewer = renewer;
    }

    public Text getRealUser() {
        return realUser;
    }

    private void setRealUser(Text realUser) {
        if (realUser == null) {
            this.realUser = new Text();
        } else {
            this.realUser = realUser;
        }
    }

}
