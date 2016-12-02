package com.he.strom.trident;

import java.io.Serializable;

public class DiagnosisEvent implements Serializable{

	/**
	 * serialVersionUID
	 */
	private static final long serialVersionUID = 6645367641468363750L;

	private Double lat;
	
	private Double lng;
	
	private Long time;
	
	private String diag;
	
	public Double getLat() {
		return lat;
	}

	public void setLat(Double lat) {
		this.lat = lat;
	}

	public Double getLng() {
		return lng;
	}

	public void setLng(Double lng) {
		this.lng = lng;
	}

	public Long getTime() {
		return time;
	}

	public void setTime(Long time) {
		this.time = time;
	}

	public String getDiag() {
		return diag;
	}

	public void setDiag(String diag) {
		this.diag = diag;
	}
	
	@Override
	public String toString() {
		return "DiagnosisEvent [lat=" + lat + ", lng=" + lng + ", time=" + time + ", diag=" + diag + "]";
	}

	public DiagnosisEvent() {
		super();
	}
	
	public DiagnosisEvent(double lat, double lng, long time, String diag) {
		super();
		this.lat = lat;
		this.lng = lng;
		this.time = time;
		this.diag = diag;
	}
	
}
