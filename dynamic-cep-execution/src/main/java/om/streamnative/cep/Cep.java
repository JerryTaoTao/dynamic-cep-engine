package om.streamnative.cep;

import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class Cep {
    private static final Logger LOG = LoggerFactory.getLogger(Cep.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Cep start with parameters \n {} \n", args);
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        Map<String, String> props = parameterTool.toMap();
        CepProxy cepProxy = new CepProxy();
        cepProxy.execute(props);
    }
}
