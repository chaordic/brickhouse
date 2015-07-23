package chaordic.product;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.List;

@Description(name = "get_main_category",
    value="_FUNC_(array<struct>)"
)
public class GetMainCategoryUDF extends GenericUDF {

    private ListObjectInspector categoriesInspector;
    private StructObjectInspector categoryInspector;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentException("test only takes 1 argument");
        }

        ObjectInspector arg0 = arguments[0];

        if (!(arg0 instanceof ListObjectInspector)) {
            throw new UDFArgumentException("test only takes a list as a argument");
        }

        this.categoriesInspector = (ListObjectInspector) arg0;

        if (!(this.categoriesInspector.getListElementObjectInspector() instanceof StructObjectInspector)) {
            throw new UDFArgumentException("test takes a list of categoryInspector as a parameter");
        }

        this.categoryInspector = (StructObjectInspector) this.categoriesInspector.getListElementObjectInspector();

        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    public String evaluate(DeferredObject[] arguments) throws HiveException {
        List categories = categoriesInspector.getList(arguments[0].get());
        if (categories == null) {
            return null;
        }
        StructField parentsField = this.categoryInspector.getStructFieldRef("parents");
        for (Object category : categories) {
            if (category == null) {
                continue;
            }
            if (isRootCategory(category, parentsField)) {
                return category.toString();
            }
        }
        return null;
    }

    private boolean isRootCategory(Object category, StructField parentsField) {
        Object parents = this.categoryInspector.getStructFieldData(category, parentsField);
        boolean isRootCategory = true;
        if (parents == null) {
            isRootCategory = true;
        } else if (parents instanceof String) {
            String parentAsString = (String) parents;
            isRootCategory = parentAsString.isEmpty()
                || parentAsString.equalsIgnoreCase("null")
                || parentAsString.equalsIgnoreCase("[]");
        } else if (parents instanceof List) {
            isRootCategory = ((List) parents).isEmpty();
        }
        return isRootCategory;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return String.format("get_main_category( %s )", strings[0]);
    }
}
