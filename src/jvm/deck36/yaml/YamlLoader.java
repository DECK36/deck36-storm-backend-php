/**
* General recursive YAML loader.
*
* @author Stefan Schadwinkel <stefan.schadwinkel@deck36.de>
* @copyright Copyright (c) 2013 DECK36 GmbH & Co. KG (http://www.deck36.de)
*
* For the full copyright and license information, please view the LICENSE
* file that was distributed with this source code.
*
*/


package deck36.yaml;

import org.apache.commons.lang.reflect.MethodUtils;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


public class YamlLoader {

    public static Number updateMap(Object start, Number addendum) {
        return addendum;
    }

    public static String updateMap(Object start, String addendum) {
        return addendum;
    }

    public static Boolean updateMap(Object start, Boolean addendum) {
        return addendum;
    }

    public static List updateMap(List start, List addendum) {

        LinkedList result = new LinkedList(start);
        result.addAll(addendum);
        return result;
    }

    public static Map updateMap(Map resultYamlMap, Map<String, Object> localYamlMap) {

        for (Map.Entry<String, Object> entry : localYamlMap.entrySet()) {

            if (resultYamlMap.containsKey(entry.getKey())) {
                System.out.println(entry.getKey());

                try {

                    Object startValue = resultYamlMap.get(entry.getKey());
                    Object updateValue = entry.getValue();

                    if (startValue == null) {
                        resultYamlMap.put(entry.getKey(), updateValue);
                    } else {
                        resultYamlMap.put(entry.getKey(),
                                MethodUtils.invokeStaticMethod(YamlLoader.class, "updateMap", new Object[] { startValue, updateValue }));
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(1);
                }

            } else {
                resultYamlMap.put(entry.getKey(), entry.getValue());
            }

        }

        return resultYamlMap;
    }



    public static Map loadYamlFromResource(String resource) {

        InputStream yamlConfig = Thread.currentThread().getContextClassLoader().getResourceAsStream(resource);

        Yaml yaml = new Yaml();
        Map mainYamlMap = (Map) yaml.load(yamlConfig);

        Map resultYamlMap = new HashMap();

        List<Map> imports = (List<Map>) mainYamlMap.get("imports");

        if (imports != null) {
            for (Map map : imports) {
                System.out.println(map.get("resource"));
                Map<String, Object> localYamlMap = loadYamlFromResource((String) map.get("resource"));

                resultYamlMap = updateMap(resultYamlMap, localYamlMap);
            }
        }

        resultYamlMap = updateMap(resultYamlMap, mainYamlMap);

        return resultYamlMap;

    }

}



