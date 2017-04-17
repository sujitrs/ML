-- Clean up script
--Delete from predictions  
--Truncate predictions  
Select * from run_details

--Select count(*) from predictions  --Should be 126006

--Select count(*) from predictions  
--Where rf_prediction!=final_prediction

--Select count(*) from predictions  
--Where dt_prediction!=final_prediction

--Select count(*) from predictions  
--Where lr_prediction!=final_prediction
Select 146/126000


 Select count(*) from dataset b ,predictions  a
--Select  b.label,a.lr_prediction,a.dt_prediction,a.rf_prediction,a.final_prediction from dataset b ,predictions  a
Where 
 b.label!=a.final_prediction AND
  a.duration =b.duration AND
  a.protocol_type=b.protocol_type AND 
  a.service=b.service AND 
  a.flag=b.flag AND 
  a.src_bytes=b.src_bytes AND 
  a.dst_bytes =b.dst_bytes  AND 
  a.land =b.land  AND 
  a.wrong_fragment=b.wrong_fragment AND 
  a.urgent=b.urgent AND 
  a.hot=b.hot AND 
  a.num_failed_logins =b.num_failed_logins AND 
  a.logged_in =b.logged_in  AND 
  a.num_compromised =b.num_compromised  AND 
  a.root_shell =b.root_shell  AND 
  a.su_attempted =b.su_attempted  AND 
  a.num_root =b.num_root  AND 
  a.num_file_creations =b.num_file_creations  AND 
  a.num_shells =b.num_shells AND 
  a.num_access_files =b.num_access_files AND 
  a.num_outbound_cmds =b.num_outbound_cmds  AND 
  a.is_host_login =b.is_host_login  AND 
  a.is_guest_login =b.is_guest_login AND 
  a.count =b.count  AND 
  a.srv_count =b.srv_count  AND 
  a.serror_rate =b.serror_rate AND 
  a.srv_serror_rate =b.srv_serror_rate AND 
  a.rerror_rate =b.rerror_rate AND 
  a.srv_rerror_rate =b.srv_rerror_rate AND 
  a.same_srv_rate =b.same_srv_rate  AND 
  a.diff_srv_rate =b.diff_srv_rate  AND 
  a.srv_diff_host_rate =b.srv_diff_host_rate  AND 
  a.dst_host_count =b.dst_host_count AND 
  a.dst_host_srv_count =b.dst_host_srv_count AND 
  a.dst_host_same_srv_rate =b.dst_host_same_srv_rate AND 
  a.dst_host_diff_srv_rate =b.dst_host_diff_srv_rate AND 
  a.dst_host_same_src_port_rate =b.dst_host_same_src_port_rate AND 
  a.dst_host_srv_diff_host_rate =b.dst_host_srv_diff_host_rate AND 
  a.dst_host_serror_rate =b.dst_host_serror_rate AND 
  a.dst_host_srv_serror_rate =b.dst_host_srv_serror_rate AND 
  a.dst_host_rerror_rate =b.dst_host_rerror_rate AND 
  a.dst_host_srv_rerror_rate =b.dst_host_srv_rerror_rate AND 
  a.field1 =b.field1 AND 
  a.field2 =b.field2

