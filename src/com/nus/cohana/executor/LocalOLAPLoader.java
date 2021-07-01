package com.nus.cohana.executor;

import com.nus.cohana.core.OLAP.Node;
import com.nus.cohana.core.iceberg.aggregator.AggregatorFactory;
import com.nus.cohana.core.iceberg.query.Aggregation;
import com.nus.cohana.core.iceberg.query.IcebergQuery;
import com.nus.cohana.core.iceberg.query.SelectionQuery;
import com.nus.cohana.core.io.readstore.CubeRS;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.*;

public class LocalOLAPLoader {

    private Map<String, Map<List<String>, Node>> dims = new HashMap<>();

    private Map<String, Node> selectedDims = new HashMap<>();

    private List<Aggregation> aggregations = new ArrayList<>();

    private Node rollUp(String dim, String level, String value) {
        Node node = this.dims.get(dim).get(Arrays.asList(level, value)).getParent();
        this.selectedDim(dim, node);
        return node;
    }

    private List<Node> drillDown(String dim, String level, String value) {
        return this.dims.get(dim).get(Arrays.asList(level, value)).getChildren();
    }

    public void init(String dirPath) throws IOException {
        File dir = new File(dirPath);
        File[] nodeFiles = dir.listFiles();
        for (File nodeFile : nodeFiles) {
            String nodeName = nodeFile.getName().split("\\.")[0];
            Map node = Node.readFrom(nodeFile.getPath());
            this.dims.put(nodeName, node);
        }
        for (Map.Entry entry : this.dims.entrySet()) {
            Map<List<String>, Node> map = (Map<List<String>, Node>) entry.getValue();
            this.selectedDims.put((String) entry.getKey(), map.get(Arrays.asList("root", "root")));
        }
    }

    private IcebergQuery generateQuery(String timeRange, IcebergQuery.granularityType granularity) {
        IcebergQuery query = new IcebergQuery();
        query.setTimeRange(timeRange);
        query.setGranularity(granularity);

        SelectionQuery selection = new SelectionQuery();
        selection.setType(SelectionQuery.SelectionType.and);
        List<SelectionQuery> fields = new ArrayList<>();
        List<String> groupFields = new ArrayList<>();
        for (Map.Entry entry : this.selectedDims.entrySet()) {
            Node node = (Node) entry.getValue();
            if (node.getLevel().equals("root")) continue;
            SelectionQuery field = new SelectionQuery();
            field.setType(SelectionQuery.SelectionType.filter);
            field.setDimension(node.getLevel());
            field.setValues(Collections.singletonList(node.getValue()));
            fields.add(field);
            groupFields.add(node.getChildLevel());
        }
        selection.setFields(fields);
        query.setSelection(selection);
        query.setGroupFields(groupFields);

        query.setAggregations(this.aggregations);
        return query;
    }

    public void selectedDim(String dim, Node node) {
        this.selectedDims.put(dim, node);
    }

    public QueryResult executeQuery(CubeRS cube, String timeRange, IcebergQuery.granularityType granularity) throws ParseException {
        IcebergQuery query = generateQuery(timeRange, granularity);
        System.out.println(query.toPrettyString());
        return LocalIcebergLoader.wrapResult(cube, query);
    }

    public void addAggregation(String fieldName, List<AggregatorFactory.AggregatorType> operators) {
        Aggregation aggregation = new Aggregation(fieldName, operators);
        this.aggregations.add(aggregation);
    }

    public static void main(String[] args) throws IOException, ParseException {
        CohortModel cohortModel = new CohortModel(args[0]);
        cohortModel.reload(args[1]);

        LocalOLAPLoader loader = new LocalOLAPLoader();
        loader.init(args[3]);
        Node node = loader.dims.get("region").get(Arrays.asList("city", "Whyalla"));
        loader.selectedDim("region", node);
        //node = loader.dims.get("category").get(Arrays.asList("product_category_3", "Motors"));
        //loader.selectedDim("category", node);
        loader.addAggregation("value", Collections.singletonList(AggregatorFactory.AggregatorType.SUM));
        QueryResult result = loader.executeQuery(cohortModel.getCube(args[2]), null, null);
        System.out.println(result.toString());
    }
}
