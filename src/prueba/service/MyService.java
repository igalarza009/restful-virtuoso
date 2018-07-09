package prueba.service;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.util.Scanner;

import javax.ws.rs.Consumes;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;

import virtuoso.jena.driver.VirtGraph;
import virtuoso.jena.driver.VirtuosoQueryExecution;
import virtuoso.jena.driver.VirtuosoQueryExecutionFactory;
import virtuoso.jena.driver.VirtuosoUpdateFactory;
import virtuoso.jena.driver.VirtuosoUpdateRequest;

import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;


@Path("/service")
public class MyService {
	
//	private String graphURI = "<http://www.sensores.com/ontology/prueba08/extrusoras#>";
	private String graphURI = "<http://www.sensores.com/ontology/pruebas_fixed/extrusoras#>";
//	private String url = "jdbc:virtuoso://localhost:1111";
	private String url = "jdbc:virtuoso://35.237.194.21:1111";

	@Path("/hello")
	@POST
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	@Produces(MediaType.TEXT_PLAIN)
	public String hello(@FormParam("name") String name) {
	 	return "Hello " + name;
	}
	   
	@Path("/query")
	@POST
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	@Produces("application/json")
	public Response postSPARQLQueryResult_JSON(@FormParam("query") String consulta){
	 	String json=null;
	 	VirtGraph set = new VirtGraph (url, "dba", "dba");
	 	System.out.println("Conexión establecida");
	 	Query query = QueryFactory.create(consulta);
	 	VirtuosoQueryExecution qe = VirtuosoQueryExecutionFactory.create(query, set);
	 	System.out.println("Ejecutamos la consulta");
	 	try {
	 		ResultSet results = qe.execSelect() ;
	 	    // write to a ByteArrayOutputStream
	 	    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
	 	    ResultSetFormatter.outputAsJSON(outputStream, results);
	 	    // and turn that into a String
	 	    json = new String(outputStream.toByteArray());
	 	    System.out.println("Guardamos los resultados en variable json");
//	 	    System.out.println(json);
	 	 } catch (Exception e) {
	 	    e.printStackTrace();
	 	 } finally {
	 	    qe.close();
	 	 }
	 	 // return json;
	 	System.out.println("Devolvemos la respuesta");
	 	 return Response.ok(json).build();
	   }
	
	
	@Path("/queryGet")
	@GET
	@Produces("application/json")
	public Response getSPARQLQueryResult_JSON(@QueryParam("query") String consulta){ //	@QueryParam
	 	String json=null;
	 	VirtGraph set = new VirtGraph (url, "dba", "dba");
	 	System.out.println("Conexión establecida");
	 	Query query = QueryFactory.create(consulta);
	 	VirtuosoQueryExecution qe = VirtuosoQueryExecutionFactory.create(query, set);
	 	System.out.println("Ejecutamos la consulta");
	 	try {
	 		ResultSet results = qe.execSelect() ;
	 	    // write to a ByteArrayOutputStream
	 	    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
	 	    ResultSetFormatter.outputAsJSON(outputStream, results);
	 	    // and turn that into a String
	 	    json = new String(outputStream.toByteArray());
	 	    System.out.println("Guardamos los resultados en variable json");
	 	 } catch (Exception e) {
	 	    e.printStackTrace();
	 	 } finally {
	 	    qe.close();
	 	 }
	 	 // return json;
	 	System.out.println("Devolvemos la respuesta");
	 	 return Response.ok(json).build();
	   }
	   
	   @Path("/insert")
//	   @GET
	   @POST
	   @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	   @Produces("application/json")//Mirar cómo quitar o modificar esto  
	   public Response insertRDF(@FormParam("query") String query){	//@QueryParam    
	 	    //String query = "INSERT DATA {graph <prueba01> {<#book7> <#price> 47 .}}";	    
	 	    VirtGraph set = new VirtGraph (url, "dba", "dba");
	 	    VirtuosoUpdateRequest vur = VirtuosoUpdateFactory.create(query, set);
	 	    vur.exec(); 
	 	    return Response.ok().build();
	   }
	   
	   @Path("/bulkinsert")
	   @GET
	   @Produces("application/json")//Mirar cómo quitar o modificar esto   
	   public Response bulkInsertRDF(@QueryParam("file") String file, @QueryParam("graph") String graph) throws FileNotFoundException{	    
	    	String query="", linea;
	 	    VirtuosoUpdateRequest vur;
	     
	 	    VirtGraph set = new VirtGraph (url, "dba", "dba");
	 	    
	 	    @SuppressWarnings("resource")
	 		Scanner input = new Scanner(new FileReader("data/"+file));
	 	    while(input.hasNext()){
	 	    	linea=input.nextLine();	    	
	 	    	query = "INSERT DATA ";
	 	    	if(graph!=null){
	 	    		query = query + "{graph <"+graph+"> ";	    	
	 	    	}
	 	    	query = query + "{"+linea+"}";
	 	    	if(graph!=null){
	 	    		query = query+"}";	    	
	 	    	}
	 	    	System.out.println(query);
	 	    	vur= VirtuosoUpdateFactory.create(query, set);
	 	    	vur.exec();	    	
	 	    }

	 	    return Response.ok().build();
	   }
	   
//	   ---------- EJEMPLO DEL ARCHIVO TTL ------------
//		   
//		@prefix : <http://www.sensores.com/ontology/prueba03/extrusoras#> . 
//		@prefix owl: <http://www.w3.org/2002/07/owl#> . 
//		@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> . 
//		@prefix xsd: <http://www.w3.org/2001/XMLSchema#> . 
//		@prefix sosa: <http://www.w3.org/ns/sosa/> . 
//		@base <http://www.sensores.com/ontology/prueba03/extrusoras#> . 
//		:sensor2F1KT7date20180324obs1 rdf:type owl:NamedIndividual , 
//		:TemperatureObservation . 
//		:sensor2F1KT7date20180324obs1result rdf:type owl:NamedIndividual , 
//		:DoubleValueResult . 
//		:sensor2F1KT7date20180324obs1result sosa:hasSimpleResult "195.9"^^xsd:double . 
//		:sensor2F1KT7date20180324obs1 sosa:hasResult :sensor2F1KT7date20180324obs1result . 
//		:sensor2F1KT7date20180324obs1 sosa:resultTime "2018-03-24T23:59:59.657Z"^^xsd:dateTime . 
//		:sensor2F1KT7 sosa:madeObservation :sensor2F1KT7date20180324obs1 . 
//	   
//	   --------------- EJEMPLO DE INSERT ----------------
//	   
//	   	prefix : <http://www.sensores.com/ontology/prueba03/extrusoras#>
//		prefix owl: <http://www.w3.org/2002/07/owl#>
//		prefix sosa: <http://www.w3.org/ns/sosa/>
//		prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
//		prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
//
//		insert data {
//		   	graph <http://www.sensores.com/ontology/prueba03/extrusoras#>
//		   	{
//		   		:sensor2F1KT7obs1prueba rdf:type owl:NamedIndividual , 
//		   										:TemperatureObservation . 
//		   		:sensor2F1KT7obs1pruebaResult rdf:type owl:NamedIndividual , 
//		   										:DoubleValueResult .
//		   		:sensor2F1KT7obs1pruebaResult sosa:hasSimpleResult "5555"^^xsd:double .
//		   		:sensor2F1KT7obs1prueba sosa:hasResult :sensor2F1KT7obs1pruebaResult .
//		   		:sensor2F1KT7obs1prueba sosa:resultTime "2018-05-15T23:05:55.555Z" .
//		   		:sensor2F1KT7 sosa:madeObservation :sensor2F1KT7obs1prueba . 
//		   	}
//		}

	   
	   @Path("/insertfile")
	   @POST
//	   @GET
	   @Consumes(MediaType.MULTIPART_FORM_DATA)
	   @Produces("application/json")//Mirar cómo quitar o modificar esto   
	   public Response bulkInsertRDF_2(
			   				@FormDataParam("file") InputStream uploadedInputStream,
			   				@FormDataParam("file") FormDataContentDisposition fileDetail){
		   Timestamp timestamp = new Timestamp(System.currentTimeMillis());
	       System.out.println("HORA INICIAL: " + timestamp);
		   VirtuosoUpdateRequest vur;  
	 	   VirtGraph set = new VirtGraph (url, "dba", "dba");
		   try
		   {
		       String line;
		       BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(uploadedInputStream));
		       String prefixes = "";
		       String queries = "";
		       int contador = 0;
		       int i = 0;
		       String insertQuery = "";
		       while( (line = bufferedReader.readLine()) != null )
		       { 
		    	   contador++;
		    	   if (line.indexOf('@') != -1) {
		    		   if (!line.contains("base")) {
		    			   prefixes += line.substring(1, line.length()-2);
		    		   }
		    	   }
		    	   else {
		    		   queries += line + " ";
		    	   }
		    	   
		    	   if (contador == 500) {
		    		   insertQuery = prefixes;
		    		   insertQuery += "insert data { graph " + graphURI + " { ";
		    		   insertQuery += queries;
		    		   insertQuery += "} } ";
		    		   i++;
//		    		   System.out.println("Ejecutamos query " + i);
		    		   vur= VirtuosoUpdateFactory.create(insertQuery, set);
		    		   vur.exec();	
		    		   contador = 0;
		    		   queries = "";
		    	   }
		       } 
		       if (contador > 0) {
		    	   insertQuery = prefixes;
	    		   insertQuery += "insert data { graph " + graphURI + " { ";
	    		   insertQuery += queries;
	    		   insertQuery += "} } ";
//	    		   System.out.println("Ejecutamos query final");
	    		   vur= VirtuosoUpdateFactory.create(insertQuery, set);
	    		   vur.exec();	
			   }
		   } 
		   catch( IOException e )
		   {
		       System.err.println( "Error: " + e );
		   }

		   timestamp = new Timestamp(System.currentTimeMillis());
	       System.out.println("HORA FINAL: " + timestamp);
	 	    return Response.ok().build();
	   }
	   
	   @Path("/insertprueba")
	   @POST
	   @Produces("application/json")
	   public Response bulkInsertRDF_Prueba(){
		   Timestamp timestamp = new Timestamp(System.currentTimeMillis());
	       System.out.println("HORA INICIAL: " + timestamp);
		   VirtuosoUpdateRequest vur;  
	 	   VirtGraph set = new VirtGraph (url, "dba", "admin");
		   try
		   {
			   String insertQuery = "PREFIX dc: <http://purl.org/dc/elements/1.1/> " +
					   "insert into " +
					   "graph <http://www.pruebas.com/maquina_virtual/prueba_insert#> " + 
					   "{ " +
					   "<http://example/egbook> dc:title  \"Probando probando...\" . " +
					   "} ";
			   vur= VirtuosoUpdateFactory.create(insertQuery, set);
			   vur.exec();	
		    } 
		   catch(Exception e)
		   {
		       System.err.println( "Error: " + e );
		   }

		   timestamp = new Timestamp(System.currentTimeMillis());
	       System.out.println("HORA FINAL: " + timestamp);
	 	    return Response.ok().build();
	   }
	
	
}
