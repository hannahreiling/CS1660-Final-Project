import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.gax.paging.Page;
import com.google.api.services.dataproc.Dataproc;
import com.google.api.services.dataproc.model.HadoopJob;
import com.google.api.services.dataproc.model.Job;
import com.google.api.services.dataproc.model.JobPlacement;
import com.google.api.services.dataproc.model.SubmitJobRequest;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.awt.*;
import java.awt.event.*;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import javax.swing.*;


public class UserInterface {

    private JFrame frame;
    private boolean shakespeare = false;
    private boolean tolstoy = false;
    private boolean hugo = false;
    private boolean all = false;
    
    /**
     * Launch the application.
     */
    public static void main(String[] args) {
        EventQueue.invokeLater(new Runnable() {
            public void run() {
                try {
                    UserInterface window = new UserInterface();
                    window.frame.setVisible(true);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    /**
     * Create the application.
     */
    public UserInterface() {
        initialize();
    }

    /**
     * Initialize the contents of the frame.
     */
    private void initialize() {
        frame = new JFrame();
        frame.setBounds(100, 100, 450, 300);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.getContentPane().setLayout(null);
        
        //welcome message
        JLabel lblRunInvertedIndex = new JLabel("Welcome! Select files to run inverted index on:");
        lblRunInvertedIndex.setBounds(31, 6, 389, 63);
        frame.getContentPane().add(lblRunInvertedIndex);

        ButtonGroup G1 = new ButtonGroup();
        
        //button to include shakespeare files
        JRadioButton buttonShakespeare = new JRadioButton("Shakespeare");
        G1.add(buttonShakespeare);
        buttonShakespeare.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                //indicate shakespeare is selected and deselect the rest
                if(buttonShakespeare.isSelected()) {
                    shakespeare = true;
                    all = false;
                    hugo = false;
                    tolstoy = false;
                } 
                else
                    shakespeare = false;
            }
        });
        buttonShakespeare.setBounds(154, 81, 128, 23);
        frame.getContentPane().add(buttonShakespeare);
        
        //button to include tolstoy files
        JRadioButton buttonTolstoy = new JRadioButton("Tolstoy");
        G1.add(buttonTolstoy);
        buttonTolstoy.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                //indicate tolstoy is selected and deselect the rest
                if(buttonTolstoy.isSelected()) {
                    tolstoy = true;
                    all = false;
                    hugo = false;
                    shakespeare = false;
                }
                else
                    tolstoy = false;
            }
        });
        buttonTolstoy.setBounds(154, 116, 128, 23);
        frame.getContentPane().add(buttonTolstoy);
        
        //button to include hugo files
        JRadioButton buttonHugo = new JRadioButton("Hugo");
        G1.add(buttonHugo);
        buttonHugo.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                //indicate hugo is selected and deselect the rest
                if(buttonHugo.isSelected()) {
                    hugo = true;
                    all = false;
                    shakespeare = false;
                    tolstoy = false;
                }
                else
                    hugo = false;
            }
        });
        buttonHugo.setBounds(154, 151, 128, 23);
        frame.getContentPane().add(buttonHugo);
        
        //button to include all provided files
        JRadioButton buttonAllFiles = new JRadioButton("All files");
        G1.add(buttonAllFiles);
        buttonAllFiles.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                //indicate all files is selected and deselect the rest
                if(buttonAllFiles.isSelected()) {
                    all = true;
                    hugo = false;
                    shakespeare = false;
                    tolstoy = false;
                }
                else
                    all = false;
            }
        });
        buttonAllFiles.setBounds(154, 186, 128, 23);
        frame.getContentPane().add(buttonAllFiles);
        
        //button to run inverted indices on selected files
        JButton btnRunInvertedIndex = new JButton("Run Inverted Index");
        btnRunInvertedIndex.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                StringBuffer output = new StringBuffer();
                //gcp project info
                String projectId = "cs1660-project-274018";
                String cluster = "cluster-aaa3";

                //arg info for submitting job
                //input data folder
                String arg1 = "gs://dataproc-staging-us-west1-941194434674-mzjezdd0/";
                //output folder
                String arg2 = "gs://dataproc-staging-us-west1-941194434674-mzjezdd0/";
                String outputDir = "";
                Random rand = new Random();
                int rand_int = rand.nextInt(1000000000);
                if(shakespeare) {
                    arg1 = arg1 + "shakespeare";
                    outputDir = "shakespeare-ouput-" + rand_int;
                    arg2 = arg2 + outputDir;
                } else if(hugo) {
                    arg1 = arg1 + "Hugo";
                    outputDir = "hugo-output-" + rand_int;
                    arg2 = arg2 + outputDir;
                } else if(tolstoy) {
                    arg1 = arg1 + "Tolstoy";
                    outputDir = "tolstoy-output-" + rand_int;
                    arg2 = arg2 + outputDir;
                } else {
                    arg1 = arg1 + "Data";
                    outputDir = "data-output-" + rand_int;
                    arg2 = arg2 + outputDir;
                }

                try {
                    //submit job to gcp
                    //modified from https://stackoverflow.com/questions/35611770/how-do-you-use-the-google-dataproc-java-client-to-submit-spark-jobs-using-jar-fi
                    //and discussion board posts
                    InputStream inputStream = this.getClass().getResourceAsStream("/credentials.json");
                    GoogleCredentials credentials = GoogleCredentials.fromStream(inputStream)
                        .createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));
                    HttpRequestInitializer requestInitializer = new HttpCredentialsAdapter(credentials);
                    Dataproc dataproc = new Dataproc.Builder(new NetHttpTransport(),new JacksonFactory(), requestInitializer)
                        .setApplicationName("inverted-index")
                        .build();
                    Job submittedJob = dataproc.projects().regions().jobs().submit(
                        projectId, "us-west1", new SubmitJobRequest()
                            .setJob(new Job()
                                .setPlacement(new JobPlacement()
                                    .setClusterName(cluster))
                            .setHadoopJob(new HadoopJob()
                                .setMainClass("InvertedIndex")
                                .setJarFileUris(ImmutableList.of("gs://dataproc-staging-us-west1-941194434674-mzjezdd0/JAR"))
                                .setArgs(ImmutableList.of(
                                    arg1, arg2)))))
                    .execute();

                    //wait for job to execute to move on 
                    //modified from https://stackoverflow.com/questions/35704048/what-is-the-best-way-to-wait-for-a-google-dataproc-sparkjob-in-java
                    String jobId = submittedJob.getReference().getJobId();
                    Job job = dataproc.projects().regions().jobs().get(projectId, "us-west1", jobId).execute();

                    String status = job.getStatus().getState();
                    while (!status.equalsIgnoreCase("DONE") && !status.equalsIgnoreCase("CANCELLED") && !status.equalsIgnoreCase("ERROR")) {
                        System.out.println("Job not done yet; current state: " + job.getStatus().getState());
                        Thread.sleep(5000);
                        job = dataproc.projects().regions().jobs().get(projectId, "us-west1", jobId).execute();
                        status = job.getStatus().getState();
                    }

                    System.out.println("Job terminated in state: " + job.getStatus().getState());
                } catch(Exception err) {
                    err.printStackTrace();
                }

                //download files generated by above jobs
                //modified from https://stackoverflow.com/questions/25141998/how-to-download-a-file-from-google-cloud-storage-with-java
                //and https://cloud.google.com/storage/docs/listing-objects
                try {
                    String bucketName = "dataproc-staging-us-west1-941194434674-mzjezdd0";
                    InputStream inputStream = this.getClass().getResourceAsStream("/credentials.json");
                    GoogleCredentials credentials = GoogleCredentials.fromStream(inputStream)
                        .createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));
                    Storage storage = StorageOptions.newBuilder()
                        .setCredentials(credentials)
                        .setProjectId(projectId)
                        .build()
                        .getService();
                    Bucket bucket = storage.get(bucketName);
                    Page<Blob> blobs = bucket.list(
                        Storage.BlobListOption.prefix(outputDir));
                    for (Blob blob : blobs.iterateAll()) {
                        String blobContent = new String(blob.getContent());
                        output.append(blobContent);
                    }
                } catch(Exception err) {
                    err.printStackTrace();
                }
                
                //print out inverted indices on UI using output generated above
                if(output.length() == 0) {
                    output.append("Oops! Please select at least one of the options to construct an inverted index.");
                } 

                JTextArea textArea = new JTextArea(output.toString());
                textArea.setEditable(false);
                JScrollPane scrollPane = new JScrollPane(textArea);  
                textArea.setLineWrap(true);  
                textArea.setWrapStyleWord(true); 
                scrollPane.setPreferredSize( new Dimension( 500, 500 ) );
                JOptionPane.showMessageDialog(null, scrollPane, "Inverted Index Results", JOptionPane.YES_NO_OPTION);
            }
        });
        btnRunInvertedIndex.setBounds(135, 221, 169, 29);
        frame.getContentPane().add(btnRunInvertedIndex);
    }
}
