package org.sujeet.ml;

import java.util.HashMap;
import java.util.Map;

public class NslMap {
	static ModelMap[] protocol_type = new ModelMap[3];
	static HashMap<Integer,ModelMap[]> hm;
	private static ModelMap[] service_type= new ModelMap[70];
	private static ModelMap[] class_type= new ModelMap[23];
	private static ModelMap[] flag_type= new ModelMap[11];
	static ModelMap n;
	public static void init(){
		
		hm=new HashMap<Integer,ModelMap[]>();    
		
		for(int i=0;i<=2;i++){
			//n=new ModelMap();
			protocol_type[i]=new ModelMap();
			protocol_type[i].setIntVal(i);
		}
		
		for(int i=0;i<=69;i++){
			service_type[i]=new ModelMap();
			service_type[i].setIntVal(i);
		}
		
		for(int i=0;i<=10;i++){
			flag_type[i]=new ModelMap();
			flag_type[i].setIntVal(i);
		}
		
		for(int i=0;i<=22;i++){
			class_type[i]=new ModelMap();
			if(i==0) 	class_type[i].setIntVal(0);
			else		class_type[i].setIntVal(1);
				
		}
		
		protocol_type[0].setStrVal("tcp");
		protocol_type[1].setStrVal("udp");
		protocol_type[2].setStrVal("icmp");
		
		service_type[0].setStrVal("ftp_data");
		service_type[1].setStrVal("other");
		service_type[2].setStrVal("private");
		service_type[3].setStrVal("http");
		service_type[4].setStrVal("remote_job");
		service_type[5].setStrVal("name");
		service_type[6].setStrVal("netbios_ns");
		service_type[7].setStrVal("eco_i");
		service_type[8].setStrVal("mtp");
		service_type[9].setStrVal("telnet");
		service_type[10].setStrVal("finger");
		service_type[11].setStrVal("domain_u");
		service_type[12].setStrVal("supdup");
		service_type[13].setStrVal("uucp_path");
		service_type[14].setStrVal("Z39_50");
		service_type[15].setStrVal("smtp");
		service_type[16].setStrVal("csnet_ns");
		service_type[17].setStrVal("uucp");
		service_type[18].setStrVal("netbios_dgm");
		service_type[19].setStrVal("urp_i");
		service_type[20].setStrVal("auth");
		service_type[21].setStrVal("domain");
		service_type[22].setStrVal("ftp");
		service_type[23].setStrVal("bgp");
		service_type[24].setStrVal("ldap");
		service_type[25].setStrVal("ecr_i");
		service_type[26].setStrVal("gopher");
		service_type[27].setStrVal("vmnet");
		service_type[28].setStrVal("systat");
		service_type[29].setStrVal("http_443");
		service_type[30].setStrVal("efs");
		service_type[31].setStrVal("whois");
		service_type[32].setStrVal("imap4");
		service_type[33].setStrVal("iso_tsap");
		service_type[34].setStrVal("echo");
		service_type[35].setStrVal("klogin");
		service_type[36].setStrVal("link");
		service_type[37].setStrVal("sunrpc");
		service_type[38].setStrVal("login");
		service_type[39].setStrVal("kshell");
		service_type[40].setStrVal("sql_net");
		service_type[41].setStrVal("time");
		service_type[42].setStrVal("hostnames");
		service_type[43].setStrVal("exec");
		service_type[44].setStrVal("ntp_u");
		service_type[45].setStrVal("discard");
		service_type[46].setStrVal("nntp");
		service_type[47].setStrVal("courier");
		service_type[48].setStrVal("ctf");
		service_type[49].setStrVal("ssh");
		service_type[50].setStrVal("daytime");
		service_type[51].setStrVal("shell");
		service_type[52].setStrVal("netstat");
		service_type[53].setStrVal("pop_3");
		service_type[54].setStrVal("nnsp");
		service_type[55].setStrVal("IRC");
		service_type[56].setStrVal("pop_2");
		service_type[57].setStrVal("printer");
		service_type[58].setStrVal("tim_i");
		service_type[59].setStrVal("pm_dump");
		service_type[60].setStrVal("red_i");
		service_type[61].setStrVal("netbios_ssn");
		service_type[62].setStrVal("rje");
		service_type[63].setStrVal("X11");
		service_type[64].setStrVal("urh_i");
		service_type[65].setStrVal("http_8001");
		service_type[66].setStrVal("aol");
		service_type[67].setStrVal("http_2784");
		service_type[68].setStrVal("tftp_u");
		service_type[69].setStrVal("harvest");

		flag_type[0].setStrVal("SF");
		flag_type[1].setStrVal("S0");
		flag_type[2].setStrVal("REJ");
		flag_type[3].setStrVal("RSTR");
		flag_type[4].setStrVal("SH");
		flag_type[5].setStrVal("RSTO");
		flag_type[6].setStrVal("S1");
		flag_type[7].setStrVal("RSTOS0");
		flag_type[8].setStrVal("S3");
		flag_type[9].setStrVal("S2");
		flag_type[10].setStrVal("OTH");
				
		class_type[0].setStrVal("normal");
		class_type[1].setStrVal("neptune");
		class_type[2].setStrVal("warezclient");
		class_type[3].setStrVal("ipsweep");
		class_type[4].setStrVal("portsweep");
		class_type[5].setStrVal("teardrop");
		class_type[6].setStrVal("nmap");
		class_type[7].setStrVal("satan");
		class_type[8].setStrVal("smurf");
		class_type[9].setStrVal("pod");
		class_type[10].setStrVal("back");
		class_type[11].setStrVal("guess_passwd");
		class_type[12].setStrVal("ftp_write");
		class_type[13].setStrVal("multihop");
		class_type[14].setStrVal("rootkit");
		class_type[15].setStrVal("buffer_overflow");
		class_type[16].setStrVal("imap");
		class_type[17].setStrVal("warezmaster");
		class_type[18].setStrVal("phf");
		class_type[19].setStrVal("land");
		class_type[20].setStrVal("loadmodule");
		class_type[21].setStrVal("spy");
		class_type[22].setStrVal("perl");

		hm.put(1, protocol_type);
		hm.put(2, service_type);
		hm.put(3, flag_type);
		hm.put(41, class_type);
		
		
		
	}

}
