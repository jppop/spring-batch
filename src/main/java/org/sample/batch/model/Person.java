package org.sample.batch.model;

import org.sample.batch.csv.Column;

public class Person {

  @Column(value = "NOM", position = 2)
  private String lastName;
  @Column(value = "PRENOM", position = 1)
  private String firstName;
  @Column(value = "AGE", position = 3)
  private int age;

  private String nationalId;

  public Person() {
  }

  public Person(String firstName, String lastName, int age) {
    this.firstName = firstName;
    this.lastName = lastName;
    this.age = age;
  }

  public String getFirstName() {
    return firstName;
  }

  public void setFirstName(String firstName) {
    this.firstName = firstName;
  }

  public String getLastName() {
    return lastName;
  }

  public void setLastName(String lastName) {
    this.lastName = lastName;
  }

  public int getAge() {
    return age;
  }

  public void setAge(int age) {
    this.age = age;
  }

  public String getNationalId() {
    return nationalId;
  }

  public void setNationalId(String nationalId) {
    this.nationalId = nationalId;
  }

  @Override
  public String toString() {
    return "Person{" +
      "lastName='" + lastName + '\'' +
      ", firstName='" + firstName + '\'' +
      ", age=" + age +
      ", nationalId='" + nationalId + '\'' +
      '}';
  }

}
